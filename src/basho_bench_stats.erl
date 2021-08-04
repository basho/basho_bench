%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.    
%%
%% -------------------------------------------------------------------
-module(basho_bench_stats).

-behaviour(gen_server).

%% API
-export([start_link/0,
         exponential/1,
         run/0,
         op_complete/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("basho_bench.hrl").

-record(state, { ops,
                 start_time = os:timestamp(),
                 last_write_time = os:timestamp(),
                 report_interval,
                 errors_since_last_report = false,
                 stats_writer, stats_writer_data,
                 last_warn = {0,0,0}}).

-define(WARN_INTERVAL, 1000). % Warn once a second
%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

exponential(Lambda) ->
    -math:log(rand:uniform()) / Lambda.

run() ->
    gen_server:call({global, ?MODULE}, run).

op_complete(Op, ok, ElapsedUs) ->
    op_complete(Op, {ok, 1}, ElapsedUs);
op_complete(Op, {ok, Units}, ElapsedUs) ->
    %% Update the histogram and units counter for the op in question
   % io:format("Get distributed: ~p~n", [get_distributed()]),
    case get_distributed() of
        true ->
            gen_server:cast({global, ?MODULE}, {Op, {ok, Units}, ElapsedUs});
        false ->
            folsom_metrics:notify({latencies, Op}, ElapsedUs),
            folsom_metrics:notify({units, Op}, {inc, Units})
    end,
    ok;
op_complete(Op, Result, ElapsedUs) ->
    gen_server:call({global, ?MODULE}, {op, Op, Result, ElapsedUs}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    process_flag(priority, high),

    %% Spin up folsom, but check bear is started
    {ok, _} = application:ensure_all_started(bear),
    ok = folsom:start(),

    %% Initialize an ETS table to track error and crash counters during
    %% reporting interval
    ets:new(basho_bench_errors, [protected, named_table]),

    %% Initialize an ETS table to track error and crash counters since
    %% the start of the run
    ets:new(basho_bench_total_errors, [protected, named_table]),

    %% Get the list of operations we'll be using for this test
    F1 =
        fun({OpTag, _Count}) -> {OpTag, OpTag};
           ({Label, OpTag, _Count}) -> {Label, OpTag}
        end,
    Ops = [F1(X) || X <- basho_bench_config:get(operations, [])],

    %% Get the list of measurements we'll be using for this test
    F2 =
        fun({MeasurementTag, _IntervalMS}) -> {MeasurementTag, MeasurementTag};
           ({Label, MeasurementTag, _IntervalMS}) -> {Label, MeasurementTag}
        end,
    Measurements = [F2(X) || X <- basho_bench_config:get(measurements, [])],

    %% Setup a histogram and counter for each operation -- we only track latencies on
    %% successful operations
    [begin
         folsom_metrics:new_histogram({latencies, Op}, slide, basho_bench_config:get(report_interval)),
         folsom_metrics:new_counter({units, Op})
     end || Op <- Ops ++ Measurements],

    StatsWriter = basho_bench_config:get(stats, csv),
    {ok, StatsSinkModule} = normalize_name(StatsWriter),
    _ = (catch StatsSinkModule:module_info()),
    case code:is_loaded(StatsSinkModule) of
        {file, _} ->
            ok;
        false ->
            ?WARN("Cannot load module ~p (derived on ~p, from the config value of 'stats' or compiled default)\n",
                  [StatsSinkModule, StatsWriter])
    end,
    %% Schedule next write/reset of data
    ReportInterval = timer:seconds(basho_bench_config:get(report_interval)),

    {ok, #state{ ops = Ops ++ Measurements,
                 report_interval = ReportInterval,
                 stats_writer = StatsSinkModule,
                 stats_writer_data = StatsSinkModule:new(Ops, Measurements)}}.

handle_call(run, _From, State) ->
    %% Schedule next report
    Now = os:timestamp(),
    timer:send_interval(State#state.report_interval, report),
    {reply, ok, State#state { start_time = Now, last_write_time = Now}};
handle_call({op, Op, {error, Reason}, _ElapsedUs}, _From, State) ->
    increment_error_counter(Op),
    increment_error_counter({Op, Reason}),
    {reply, ok, State#state { errors_since_last_report = true }}.

handle_cast({Op, {ok, Units}, ElapsedUs}, State = #state{last_write_time = LWT, report_interval = RI}) ->
    Now = os:timestamp(),
    TimeSinceLastReport = timer:now_diff(Now, LWT) / 1000, %% To get the diff in seconds
    TimeSinceLastWarn = timer:now_diff(Now, State#state.last_warn) / 1000,
    if
        TimeSinceLastReport > (RI * 2) andalso TimeSinceLastWarn > ?WARN_INTERVAL  ->
            ?WARN("basho_bench_stats has not reported in ~.2f milliseconds\n", [TimeSinceLastReport]),
            {message_queue_len, QLen} = process_info(self(), message_queue_len),
            ?WARN("stats process mailbox size = ~w\n", [QLen]),
            NewState = State#state{last_warn = Now};
        true ->
            NewState = State
    end,
    folsom_metrics:notify({latencies, Op}, ElapsedUs),
    folsom_metrics:notify({units, Op}, {inc, Units}),
    {noreply, NewState};
handle_cast(_, State) ->
    {noreply, State}.

handle_info(report, State) ->
    consume_report_msgs(),
    Now = os:timestamp(),
    process_stats(Now, State),
    {noreply, State#state { last_write_time = Now, errors_since_last_report = false }}.

terminate(_Reason, #state{stats_writer=Module}=State) ->
    %% Do the final stats report and write the errors file
    process_stats(os:timestamp(), State),
    report_total_errors(State),

    Module:terminate(State#state.stats_writer_data).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

%% Uses the process dictionary to memoize checks
%% for checking if we're running in distributed mode
%% as constantly checking in with a centralized gen_server
%% would impede progress

get_distributed() ->
    case erlang:get(distribute_work) of
        undefined ->
            DistributeWork = basho_bench_config:get(distribute_work, false),
            erlang:put(distribute_work, DistributeWork),
            DistributeWork;
        DistributeWork ->
            DistributeWork
    end.

increment_error_counter(Key) ->
    ets_increment(basho_bench_errors, Key, 1).

ets_increment(Tab, Key, Incr) when is_integer(Incr) ->
    %% Increment the counter for this specific key. We have to deal with
    %% missing keys, so catch the update if it fails and init as necessary
    case catch(ets:update_counter(Tab, Key, Incr)) of
        Value when is_integer(Value) ->
            ok;
        {'EXIT', _} ->
            case ets:insert_new(Tab, {Key, Incr}) of
                true ->
                    ok;
                _ ->
                    %% Race with another load gen proc, so retry
                    ets_increment(Tab, Key, Incr)
            end
    end;
ets_increment(Tab, Key, Incr) when is_float(Incr) ->
    Old = case ets:lookup(Tab, Key) of
              [{_, Val}] -> Val;
              []         -> 0
          end,
    true = ets:insert(Tab, {Key, Old + Incr}).

error_counter(Key) ->
    lookup_or_zero(basho_bench_errors, Key).

lookup_or_zero(Tab, Key) ->
    case catch(ets:lookup_element(Tab, Key, 2)) of
        {'EXIT', _} ->
            0;
        Value ->
            Value
    end.


process_stats(Now, #state{stats_writer=Module}=State) ->
    %% Determine how much time has elapsed (seconds) since our last report
    %% If zero seconds, round up to one to avoid divide-by-zeros in reporting
    %% tools.
    Elapsed = timer:now_diff(Now, State#state.start_time) / 1000000,
    Window  = timer:now_diff(Now, State#state.last_write_time) / 1000000,

    %% Time to report latency data to our CSV files
    {Oks, Errors, OkOpsRes} =
        lists:foldl(fun(Op, {TotalOks, TotalErrors, OpsResAcc}) ->
                            {Oks, Errors} = report_latency(State, Elapsed, Window, Op),
                            {TotalOks + Oks, TotalErrors + Errors,
                             [{Op, Oks}|OpsResAcc]}
                    end, {0,0,[]}, State#state.ops),

    %% Reset units
    [folsom_metrics_counter:dec({units, Op}, OpAmount) || {Op, OpAmount} <- OkOpsRes],

    %% Write summary
    Module:process_summary(State#state.stats_writer_data,
                           Elapsed, Window, Oks, Errors),

    %% Dump current error counts to console
    case (State#state.errors_since_last_report) of
        true ->
            ErrCounts = ets:tab2list(basho_bench_errors),
            true = ets:delete_all_objects(basho_bench_errors),
            ?INFO("Errors:~p\n", [lists:sort(ErrCounts)]),
            [ets_increment(basho_bench_total_errors, Err, Count) || 
                              {Err, Count} <- ErrCounts],
            ok;
        false ->
            ok
    end.

%%
%% Write latency info for a given op to the appropriate CSV. Returns the
%% number of successful and failed ops in this window of time.
%%
report_latency(#state{stats_writer=Module}=State, Elapsed, Window, Op) ->
    Stats = folsom_metrics:get_histogram_statistics({latencies, Op}),
    Errors = error_counter(Op),
    Units = folsom_metrics:get_metric_value({units, Op}),

    Module:report_latency({State#state.stats_writer,
                                             State#state.stats_writer_data},
                                            Elapsed, Window, Op,
                                            Stats, Errors, Units),
    {Units, Errors}.

report_total_errors(#state{stats_writer=Module}=State) ->
    case ets:tab2list(basho_bench_total_errors) of
        [] ->
            ?INFO("No Errors.\n", []);
        UnsortedErrCounts ->
            ErrCounts = lists:sort(UnsortedErrCounts),
            ?INFO("Total Errors:\n", []),
            F = fun({Key, Count}) ->
                        case lists:member(Key, State#state.ops) of
                            true ->
                                ok; % per op total
                            false ->
                                ?INFO("  ~p: ~p\n", [Key, Count]),
                                Module:report_error({State#state.stats_writer,
                                                                       State#state.stats_writer_data},
                                                                      Key, Count)
                        end
                end,
            lists:foreach(F, ErrCounts)
    end.

consume_report_msgs() ->
    receive
        report ->
            consume_report_msgs()
    after 0 ->
            ok
    end.

% Assuming all stats sink modules are prefixed with basho_bench_stats_writer_
normalize_name(StatsSink) when is_atom(StatsSink) ->
    {ok, list_to_atom("basho_bench_stats_writer_" ++ atom_to_list(StatsSink))};
normalize_name(StatsSink) -> {error, {StatsSink, invalid_name}}.

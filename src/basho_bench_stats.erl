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
                 summary_file,
                 errors_file}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

exponential(Lambda) ->
    -math:log(random:uniform()) / Lambda.

run() ->
    gen_server:call(?MODULE, run).

op_complete(Op, ok, ElapsedUs) ->
    op_complete(Op, {ok, 1}, ElapsedUs);
op_complete(Op, {ok, Units}, ElapsedUs) ->
    %% Update the histogram and units counter for the op in question
    folsom_metrics:notify({latencies, Op}, ElapsedUs),
    folsom_metrics:notify({units, Op}, {inc, Units}),
    ok;
op_complete(Op, Result, ElapsedUs) ->
    gen_server:call(?MODULE, {op, Op, Result, ElapsedUs}).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    process_flag(priority, high),

    %% Spin up folsom
    folsom:start(),

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

    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, op_csv_file(X)) || X <- Ops],

    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, measurement_csv_file(X)) || X <- Measurements],

    %% Setup output file w/ counters for total requests, errors, etc.
    {ok, SummaryFile} = file:open("summary.csv", [raw, binary, write]),
    file:write(SummaryFile, <<"elapsed, window, total, successful, failed\n">>),

    %% Setup errors file w/counters for each error.  Embedded commas likely
    %% in the error messages so quote the columns.
    {ok, ErrorsFile} = file:open("errors.csv", [raw, binary, write]),
    file:write(ErrorsFile, <<"\"error\",\"count\"\n">>),

    %% Schedule next write/reset of data
    ReportInterval = timer:seconds(basho_bench_config:get(report_interval)),

    {ok, #state{ ops = Ops ++ Measurements,
                 report_interval = ReportInterval,
                 summary_file = SummaryFile,
                 errors_file = ErrorsFile}}.

handle_call(run, _From, State) ->
    %% Schedule next report
    Now = os:timestamp(),
    timer:send_interval(State#state.report_interval, report),
    {reply, ok, State#state { start_time = Now, last_write_time = Now}};
handle_call({op, Op, {error, Reason}, _ElapsedUs}, _From, State) ->
    increment_error_counter(Op),
    increment_error_counter({Op, Reason}),
    {reply, ok, State#state { errors_since_last_report = true }}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(report, State) ->
    consume_report_msgs(),
    Now = os:timestamp(),
    process_stats(Now, State),
    {noreply, State#state { last_write_time = Now, errors_since_last_report = false }}.

terminate(_Reason, State) ->
    %% Do the final stats report and write the errors file
    process_stats(os:timestamp(), State),
    report_total_errors(State),

    [ok = file:close(F) || {{csv_file, _}, F} <- erlang:get()],
    ok = file:close(State#state.summary_file),
    ok = file:close(State#state.errors_file),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

op_csv_file({Label, _Op}) ->
    Fname = normalize_label(Label) ++ "_latencies.csv",
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

measurement_csv_file({Label, _Op}) ->
    Fname = normalize_label(Label) ++ "_measurements.csv",
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

normalize_label(Label) when is_list(Label) ->
    replace_special_chars(Label);
normalize_label(Label) when is_binary(Label) ->
    normalize_label(binary_to_list(Label));
normalize_label(Label) when is_integer(Label) ->
    normalize_label(integer_to_list(Label));
normalize_label(Label) when is_atom(Label) ->
    normalize_label(atom_to_list(Label));
normalize_label(Label) when is_tuple(Label) ->
    Parts = [normalize_label(X) || X <- tuple_to_list(Label)],
    string:join(Parts, "-").

replace_special_chars([H|T]) when
      (H >= $0 andalso H =< $9) orelse
      (H >= $A andalso H =< $Z) orelse
      (H >= $a andalso H =< $z) ->
    [H|replace_special_chars(T)];
replace_special_chars([_|T]) ->
    [$-|replace_special_chars(T)];
replace_special_chars([]) ->
    [].

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


process_stats(Now, State) ->
    %% Determine how much time has elapsed (seconds) since our last report
    %% If zero seconds, round up to one to avoid divide-by-zeros in reporting
    %% tools.
    Elapsed = timer:now_diff(Now, State#state.start_time) / 1000000,
    Window  = timer:now_diff(Now, State#state.last_write_time) / 1000000,

    %% Time to report latency data to our CSV files
    {Oks, Errors, OkOpsRes} =
        lists:foldl(fun(Op, {TotalOks, TotalErrors, OpsResAcc}) ->
                            {Oks, Errors} = report_latency(Elapsed, Window, Op),
                            {TotalOks + Oks, TotalErrors + Errors,
                             [{Op, Oks}|OpsResAcc]}
                    end, {0,0,[]}, State#state.ops),

    %% Reset units
    [folsom_metrics_counter:dec({units, Op}, OpAmount) || {Op, OpAmount} <- OkOpsRes],

    %% Write summary
    file:write(State#state.summary_file,
               io_lib:format("~w, ~w, ~w, ~w, ~w\n",
                             [Elapsed,
                              Window,
                              Oks + Errors,
                              Oks,
                              Errors])),

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
report_latency(Elapsed, Window, Op) ->
    Stats = folsom_metrics:get_histogram_statistics({latencies, Op}),
    Errors = error_counter(Op),
    Units = folsom_metrics:get_metric_value({units, Op}),
    case proplists:get_value(n, Stats) > 0 of
        true ->
            P = proplists:get_value(percentile, Stats),
            Line = io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~w, ~w, ~w, ~w, ~w, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Units,
                                  proplists:get_value(min, Stats),
                                  proplists:get_value(arithmetic_mean, Stats),
                                  proplists:get_value(median, Stats),
                                  proplists:get_value(95, P),
                                  proplists:get_value(99, P),
                                  proplists:get_value(999, P),
                                  proplists:get_value(max, Stats),
                                  Errors]);
        false ->
            ?WARN("No data for op: ~p\n", [Op]),
            Line = io_lib:format("~w, ~w, 0, 0, 0, 0, 0, 0, 0, 0, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Errors])
    end,
    ok = file:write(erlang:get({csv_file, Op}), Line),
    {Units, Errors}.

report_total_errors(State) ->                          
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
                                file:write(State#state.errors_file, 
                                           io_lib:format("\"~w\",\"~w\"\n",
                                                         [Key, Count]))
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

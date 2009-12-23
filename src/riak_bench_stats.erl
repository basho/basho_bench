%% -------------------------------------------------------------------
%%
%% riak_bench: Benchmarking Suite for Riak
%%
%% Copyright (c) 2009 Basho Techonologies
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
-module(riak_bench_stats).

-behaviour(gen_server).

%% API
-export([start_link/0,
         op_complete/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_bench.hrl").

-record(state, { ops,
                 start_time,
                 last_write_time,
                 report_interval,
                 summary_file}).

%% Tracks latencies up to 5 secs w/ 250 us resolution
-define(NEW_HIST, stats_histogram:new(0, 5000000, 20000)).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

op_complete(Op, Result, ElapsedUs) ->
    gen_server:cast(?MODULE, {op, Op, Result, ElapsedUs}).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    
    %% Initialize an ETS table to track error and crash counters
    ets:new(riak_bench_errors, [protected, named_table]),

    %% Get the list of operations we'll be using for this test
    Ops = [Op || {Op, _} <- riak_bench_config:get(operations)],

    %% Setup stats instance for each operation -- we only track latencies on
    %% successful operations
    %%
    %% NOTE: Store the histograms in the process dictionary to avoid painful
    %%       copying on state updates.
    [erlang:put({latencies, Op}, ?NEW_HIST) || Op <- Ops],

    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, Op}, op_csv_file(Op)) || Op <- Ops],

    %% Setup output file w/ counters for total requests, errors, etc.
    {ok, SummaryFile} = file:open("summary.csv", [raw, binary, write]),
    file:write(SummaryFile, <<"elapsed, window, total, successful, failed\n">>),

    %% Schedule next write/reset of data
    ReportInterval = timer:seconds(riak_bench_config:get(report_interval)),
    erlang:send_after(ReportInterval, self(), report),

    Now = now(),
    {ok, #state{ ops = Ops,
                 start_time = Now,
                 last_write_time = Now,
                 report_interval = ReportInterval,
                 summary_file = SummaryFile }}.

handle_call(_Message, _From, State) ->
    {reply, ok, State}.

handle_cast({op, Op, ok, ElapsedUs}, State) ->
    %% Update the histogram for the op in question
    Hist = stats_histogram:update(ElapsedUs, erlang:get({latencies, Op})),
    erlang:put({latencies, Op}, Hist),
    {noreply, State};
handle_cast({op, Op, {error, Reason}, _ElapsedUs}, State) ->
    ?ERROR("~p: ~p\n", [Op, Reason]),
    increment_error_counter(Op),
    increment_error_counter({Op, Reason}),
    {noreply, State}.

handle_info(report, State) ->
    %% Determine how much time has elapsed (seconds) since our last report
    Now = now(),
    Elapsed = trunc(timer:now_diff(Now, State#state.start_time) / 1000000),
    Window  = trunc(timer:now_diff(Now, State#state.last_write_time) / 1000000),
    
    %% Time to report latency data to our CSV files
    {Oks, Errors} = lists:foldl(fun(Op, {TotalOks, TotalErrors}) ->
                                        {Oks, Errors} = report_latency(Elapsed, Window, Op),
                                        {TotalOks + Oks, TotalErrors + Errors}
                                end, {0,0}, State#state.ops),

    %% Reset latency histograms
    [erlang:put({latencies, Op}, ?NEW_HIST) || Op <- State#state.ops],

    %% Write summary
    file:write(State#state.summary_file,
               io_lib:format("~w, ~w, ~w, ~w, ~w\n",
                             [Elapsed,
                              Window,
                              Oks + Errors,
                              Oks,
                              Errors])),

    %% Schedule next report
    erlang:send_after(State#state.report_interval, self(), report),    
    {noreply, State#state { last_write_time = Now}}.

terminate(_Reason, State) ->
    %% Do one last report and then flush all the files
    handle_info(report, State),    
    [ok = file:close(F) || {{csv_file, _}, F} <- erlang:get()],
    ok = file:close(State#state.summary_file),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

op_csv_file(Op) ->
    Fname = lists:concat([Op, "_latencies.csv"]),
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

increment_error_counter(Key) ->
    %% Increment the counter for this specific key. We have to deal with
    %% missing keys, so catch the update if it fails and init as necessary
    case catch(ets:update_counter(riak_bench_errors, Key, 1)) of
        Value when is_integer(Value) ->
            ok;
        {'EXIT', _} ->
            true = ets:insert_new(riak_bench_errors, {Key, 1}),
            ok
    end.

error_counter(Key) ->
    case catch(ets:lookup_element(riak_bench_errors, Key, 2)) of
        {'EXIT', _} ->
            0;
        Value ->
            Value
    end.
        
%%
%% Write latency info for a given op to the appropriate CSV. Returns the
%% number of successful and failed ops in this window of time.
%%
report_latency(Elapsed, Window, Op) ->
    Hist = erlang:get({latencies, Op}),
    {Min, Mean, Max, _, _} = stats_histogram:summary_stats(Hist),
    Errors = error_counter(Op),
    Line = io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~.1f, ~.1f, ~.1f, ~.1f, ~w, ~w\n",
                         [Elapsed,
                          Window,
                          stats_histogram:observations(Hist),
                          Min,
                          Mean,
                          stats_histogram:quantile(0.500, Hist),
                          stats_histogram:quantile(0.950, Hist),
                          stats_histogram:quantile(0.990, Hist),
                          stats_histogram:quantile(0.999, Hist),
                          Max,
                          Errors]),
    ok = file:write(erlang:get({csv_file, Op}), Line),
    {stats_histogram:observations(Hist), Errors}.
                          

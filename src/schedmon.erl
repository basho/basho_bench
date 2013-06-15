%% -------------------------------------------------------------------
%%
%% schedmon: periodically restart Erlang schedulers based scheduler queue imbalances
%%
%% Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
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
-module(schedmon).
-compile(export_all).
-behaviour(gen_server).

-export([start/0, start/1, start/2, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% This gen_server tries to detect an anomaly where a subset of Erlang
%% schedulers end up handling all the load of the system and the other
%% schedulers appear to be suspended or asleep. The code examines the
%% scheduler queues of all schedulers after creating a large number of
%% CPU-intensive do-nothing-harmful-except-for-CPU-consumption,
%% and if it detects chronic imbalance, it temporarily takes
%% the majority of Erlang schedulers offline and then brings them back
%% online, which as we've observed in practice clears up the imbalance
%% of load across Erlang schedulers.

%% SAMPLE_RATE controls how often the CPU load is sampled to try to detect
%% offline schedulers.
-define(SAMPLE_RATE, 150*1000).  % in milliseconds

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          schedulers :: integer(),
          sample_rate :: integer(),
          enforce_p :: boolean()
         }).

start() ->
    start(?SAMPLE_RATE, true).

start(SampleRate) ->
    start(SampleRate, true).

start(SampleRate, EnforceP) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [SampleRate, EnforceP], []).

stop() ->
    gen_server:cast(?MODULE, stop).

init([SampleRate, EnforceP]) ->
    Scheds = erlang:system_info(schedulers_online),
    erlang:send_after(SampleRate, self(), check),
    {ok, #state{schedulers=Scheds, sample_rate=SampleRate, enforce_p=EnforceP}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, #state{schedulers = Schedulers} = State) ->
    case timer:tc(fun() -> detect_balance(Schedulers, 8, 25, 40) end) of
        {Elapsed, {true, SchedQs, Ratio}} ->
            error_logger:info_msg("~p: check: balanced in ~p msec @ ~.2.0f ~p",
                                  [?MODULE, Elapsed div 1000, Ratio, SchedQs]);
        {Elapsed, {false, SchedQs, Ratio}} ->
            error_logger:info_msg("~p: check: not balanced in ~p msec @ ~.2.0f ~p",
                                  [?MODULE, Elapsed div 1000, Ratio, SchedQs]),
            gather_diagnostics(Schedulers),
            reset_schedulers(Schedulers, State#state.enforce_p)
    end,
    erlang:send_after(State#state.sample_rate, self(), check),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal functions
gather_diagnostics(Scheds) ->
    SchedState = erlang:system_info(schedulers_state),
    Blockers = erlang:system_info(multi_scheduling_blockers),
    Stats = erlang:system_info(scheduling_statistics),
    RunQs = [begin timer:sleep(100), statistics(run_queues) end ||
                _ <- lists:seq(1, 5*10)],
    _ = erlang:system_info(thread_progress),

    error_logger:info_msg("diagnostics: Scheds ~p\n", [Scheds]),
    error_logger:info_msg("diagnostics: SchedState ~p\n", [SchedState]),
    error_logger:info_msg("diagnostics: Blockers ~p\n", [Blockers]),
    error_logger:info_msg("diagnostics: Stats ~p\n", [Stats]),
    error_logger:info_msg("diagnostics: RunQs ~p\n", [RunQs]),
    error_logger:info_msg("diagnostics: check the erlang.log.X file for "
                          "'thread progress' output\n"),
    ok.

-spec reset_schedulers(integer(), boolean()) -> ok.
reset_schedulers(Scheds, false) ->
    error_logger:info_msg("~p: reset_schedulers: No action taken for ~p",
                          [?MODULE, Scheds]);
reset_schedulers(Scheds, true) ->
    error_logger:info_msg("~p: reset_schedulers: taking action for ~p",
                          [?MODULE, Scheds]),
    Scheds = erlang:system_flag(schedulers_online, 1),
    timer:sleep(250),
    _ = erlang:system_flag(schedulers_online, Scheds),
    ok.

%% Using sched_balance:t(6, 25, 40) (25 * 80 = 2000 msec limit)
%% appears to get a pretty consistent answer in under 800 msec.
%% Using Multiple=8 seems even better, but I haven't tried measuring
%% the latency impact on other (unrelated) Erlang processes during
%% the time that it's running.
%%
%% Variations in the +swt flag appear to have little effect on the
%% typical elapsed time to get a {true, ...} result.

detect_balance(Schedulers, Multiple, WaitMs, WaitPeriods) ->
    Eater = fun(F) -> F(F) end,
    Pids = [spawn_link(fun() -> Eater(Eater) end) ||
               _ <- lists:seq(1, Schedulers * Multiple)],
    C = fun(_, {true, _} = Acc) ->
                Acc;
           (_, _) ->
                Qs = erlang:statistics(run_queues),
                case lists:sort(tuple_to_list(Qs)) of
                    [0|_] ->
                        %% Still at least one scheduler with empty queue
                        timer:sleep(WaitMs),
                        {false, Qs};
                    _ ->
                        {true, Qs}
                end
        end,
    {_, Qs} = R = lists:foldl(C, false, lists:seq(1, WaitPeriods)),
    LQs = tuple_to_list(Qs),
    Max = lists:max(LQs),
    Median = erlang:max(1, lists:nth(length(LQs) div 2, lists:sort(LQs))),
    Ratio = Max / Median,
    [begin unlink(P), exit(P, kill) end || P <- Pids],
    case R of
        {true, Qs} ->
            {true, Qs, Ratio};
        {false, Qs} when Ratio < 2 ->
            {true, Qs, Ratio};    % Close enough.
        _ ->
            {false, Qs, Ratio}
    end.

-ifdef(TEST).

-endif.

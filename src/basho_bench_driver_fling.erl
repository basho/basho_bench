%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2015 Basho Techonologies
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
%% @doc This is a driver for the fling cache library.

-module(basho_bench_driver_fling).
-export([
         new/1,
         run/4
        ]).

-export([
         fling_get_key/1,
         fling_get_value/1
        ]).

-record(state, {
          pid,
          modname,
          tid,
          mode = ets,
          write = true
               }).

-define(FLING_TID, '_bb_fling_tid').
-define(FLING_PID, '_bb_fling_pid').

fling_get_key({K, _V}) -> K.
fling_get_value({_K, V}) -> V.

check_mailbox(State = #state{ pid = Pid }) ->
    receive
        nowrite ->
            State#state{ write = false };
        checkmode ->
            State#state{ mode = fling:mode(Pid) };
        Other ->
            lager:warning("Got unknown message ~p", [Other]),
            State
    after 0 ->
              State
    end.

%% Initialize app
new(1) ->
    application:load(fling),
    application:set_env(fling, max_ticks, 5),
    application:set_env(fling, secs_per_tick, 1),
    ok = application:start(fling),

    ModName = basho_bench_config:get(fling_modname, 'bb_fling$1'),
    TabName = basho_bench_config:get(fling_etsname, 'bb_fling'),
    Tid = ets:new(TabName, [{read_concurrency, true}, {write_concurrency, true}]),
    Pid = fling:manage(Tid, fun fling_get_key/1, fun fling_get_value/1, ModName),

    %% yes, evil to share state through application params.
    %% like you haven't done worse...
    application:set_env(fling, ?FLING_TID, Tid),
    application:set_env(fling, ?FLING_PID, Pid),

    erlang:send_after(1500, self(), nowrite),
    erlang:send_after(15000, self(), checkmode),
    {ok, #state{tid = Tid, modname = ModName, pid = Pid}};

new(_Id) ->
    {ok, Tid} = application:get_env(fling, ?FLING_TID),
    {ok, Pid} = application:get_env(fling, ?FLING_PID),
    ModName = basho_bench_config:get(fling_modname, 'bb_fling$1'),
    erlang:send_after(1500, self(), nowrite),
    erlang:send_after(15000, self(), checkmode),
    {ok, #state{tid = Tid, modname = ModName, pid = Pid}}.

run(put, _KeyGen, _ValueGen, State = #state{ write = false }) ->
    {ok, State};
run(put, KeyGen, ValueGen, State = #state{ write = true, pid = Pid }) ->
    Obj = {KeyGen(), ValueGen()},
    fling:put(Pid, Obj),
    NewState = check_mailbox(State),
    {ok, NewState};
run(get, KeyGen, _ValueGen, State = #state{ mode = ets, tid = Tid, modname = Mod }) ->
    fling:get(ets, Tid, Mod, KeyGen()),
    NewState = check_mailbox(State),
    {ok, NewState};
run(get, KeyGen, _ValueGen, State = #state{ mode = mg, tid = Tid, modname = Mod }) ->
    fling:get(mg, Tid, Mod, KeyGen()),
    {ok, State}.

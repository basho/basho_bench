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
          tid%,
          %mode = ets,
          %write = true
               }).

-define(FLING_TID, '_bb_fling_tid').
-define(FLING_PID, '_bb_fling_pid').

fling_get_key({K, _V}) -> K.
fling_get_value({_K, V}) -> V.

%check_mailbox(State = #state{ tid = T, modname = M }) ->
%    receive
%        nowrite ->
%            State#state{ write = false };
%        checkmode ->
%            {Mode, _} = fling:mode(T, M),
%            State#state{ mode = Mode };
%        Other ->
%            lager:warning("Got unknown message ~p", [Other]),
%            State
%    after 0 ->
%              State
%    end.

generate_data(K, V, N) ->
    gen(K, V, [], N).

gen(_K, _V, Acc, 0) ->
    Acc;
gen(K, V, Acc, N) ->
    gen(K, V, [ {K(), V()} | Acc ], N-1).

wait_until(_F, 0) -> timeout;
wait_until(F, N) ->
    case F() of
        true ->
            ok;
        false ->
            timer:sleep(1000),
            wait_until(F, N-1)
    end.

mode_is_mg({mg, _}) -> true;
mode_is_mg({ets, _}) -> false.

new(1) ->
    application:load(fling),
    application:set_env(fling, max_ticks, 2),
    application:set_env(fling, secs_per_tick, 1),
    ok = application:start(fling),

    ModName = basho_bench_config:get(fling_modname, 'bb_fling$1'),
    TabName = basho_bench_config:get(fling_etsname, 'bb_fling'),
    EtsOptions = basho_bench_config:get(fling_ets_options, []),
    Tid = ets:new(TabName, EtsOptions),

    %% We're going to generate our data now and promote it, then measure
    %% gets
    KeyGenParams = basho_bench_config:get(key_generator, {uniform_int, 1000}),
    KeyGen = basho_bench_keygen:new(KeyGenParams, 1),
    ValGenParams = basho_bench_config:get(value_generator, {fixed_bin, 100}),
    ValGen = basho_bench_valgen:new(ValGenParams, 1),

    Data = generate_data(KeyGen, ValGen, 1000),
    ets:insert(Tid, Data),

    Pid = fling:manage(Tid, fun fling_get_key/1, fun fling_get_value/1, ModName),
    wait_until(fun() -> mode_is_mg(fling:mode(Tid, ModName)) end, 20),

    %% yes, evil to share state through application params.
    %% like you haven't done worse...
    application:set_env(fling, ?FLING_TID, Tid),
    application:set_env(fling, ?FLING_PID, Pid),

    {ok, #state{tid = Tid, modname = ModName, pid = Pid}};

new(_Id) ->
    {ok, Tid} = application:get_env(fling, ?FLING_TID),
    {ok, Pid} = application:get_env(fling, ?FLING_PID),
    ModName = basho_bench_config:get(fling_modname, 'bb_fling$1'),
    {ok, #state{tid = Tid, modname = ModName, pid = Pid}}.

run(get_ets, KeyGen, _ValueGen, State = #state{tid = Tid}) ->
    fling:get({ets, Tid}, KeyGen()),
    {ok, State};
run(get_mg, KeyGen, _ValueGen, State = #state{modname = Mod}) ->
    fling:get({mg, Mod}, KeyGen()),
    {ok, State}.

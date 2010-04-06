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
-module(basho_bench_keygen).

-export([new/2,
         dimension/1]).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new({sequential_int, MaxKey}, _Id) ->
    Ref = make_ref(),
    fun() -> sequential_int_generator(Ref, MaxKey) end;
new({sequential_int_bin, MaxKey}, _Id) ->
    Ref = make_ref(),
    fun() -> Key = sequential_int_generator(Ref, MaxKey), <<Key:32/native>> end;
new({sequential_int_str, MaxKey}, _Id) ->
    Ref = make_ref(),
    fun() -> Key = sequential_int_generator(Ref, MaxKey), integer_to_list(Key) end;
new({uniform_int_bin, MaxKey}, _Id) ->
    fun() -> Key = random:uniform(MaxKey), <<Key:32/native>> end;
new({uniform_int_str, MaxKey}, _Id) ->
    fun() -> Key = random:uniform(MaxKey), integer_to_list(Key) end;
new({uniform_int, MaxKey}, _Id) ->
    fun() -> random:uniform(MaxKey) end;
new({pareto_int, Mean, Shape}, _Id) ->
    S1 = (-1 / Shape) - 1,
    S2 = Mean * (Shape - 1),
    fun() -> trunc(math:pow(1 - random:uniform(), S1) * S2) end;
new({pareto_int_bin, Mean, Shape}, _Id) ->
    S1 = (-1 / Shape) - 1,
    S2 = Mean * (Shape - 1),
    fun() -> Key = trunc(math:pow(1 - random:uniform(), S1) * S2), <<Key:32/native>> end;
new(Other, _Id) ->
    ?FAIL_MSG("Unsupported key generator requested: ~p\n", [Other]).


dimension({sequential_int, MaxKey}) ->
    MaxKey;
dimension({sequential_int_bin, MaxKey}) ->
    MaxKey;
dimension({sequential_int_str, MaxKey}) ->
    MaxKey;
dimension({uniform_int_bin, MaxKey}) ->
    MaxKey;
dimension({uniform_int_str, MaxKey}) ->
    MaxKey;
dimension({uniform_int, MaxKey}) ->
    MaxKey;
dimension({pareto_int, _, _}) ->
    0.0;
dimension({pareto_int_bin, _, _}) ->
    0.0;
dimension(Other) ->
    ?FAIL_MSG("Unsupported key generator dimension requested: ~p\n", [Other]).




%% ====================================================================
%% Internal functions
%% ====================================================================

sequential_int_generator(Ref, MaxValue) ->
   %% A bit of evil here. We want to generate numbers in sequence and stop
   %% at MaxKey. This means we need state in our anonymous function. Use the process
   %% dictionary to keep track of where we are.
   case erlang:get({sigen, Ref}) of
       undefined ->
           erlang:put({sigen, Ref}, 1),
           0;
       MaxValue ->
           throw({stop, empty_keygen});
       Value ->
           erlang:put({sigen, Ref}, Value+1),
           Value
   end.

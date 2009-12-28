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
-module(riak_bench_keygen).

-export([new/2,
         dimension/1]).

-include("riak_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new({uniform_int, MaxKey}, _Id) ->
    fun() -> Key = random:uniform(MaxKey), <<Key:32/native>> end;
new(Other, _Id) ->
    ?FAIL_MSG("Unsupported key generator requested: ~p\n", [Other]).


dimension({uniform_int, MaxKey}) ->
    MaxKey;
dimension(Other) ->
    ?FAIL_MSG("Unsupported key generator dimension requested: ~p\n", [Other]).

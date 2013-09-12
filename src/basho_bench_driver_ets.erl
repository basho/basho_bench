%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2013 Basho Techonologies
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
-module(basho_bench_driver_ets).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    Opts = basho_bench_config:get(ets_table_options, [set,private]),
    Tid = ets:new(?MODULE, Opts),
    {ok, Tid}.

run(get, KeyGen, _ValueGen, State) ->
    Tid = State,
    Key = KeyGen(),
    case ets:lookup(Tid, Key) of
        [] ->
            {ok, State};
        [{Key, _}|_] ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Tid = State,
    true = ets:insert(Tid, {KeyGen(), ValueGen()}),
    {ok, State};
run(delete, KeyGen, _ValueGen, State) ->
    Tid = State,
    true = ets:delete(Tid, KeyGen()),
    {ok, State}.
    

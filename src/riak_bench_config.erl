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
-module(riak_bench_config).

-export([load/1,
         set/2,
         get/1]).

-include("riak_bench.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

load(File) ->
    case file:consult(File) of
        {ok, Terms} ->
            load_config(Terms);
        {error, Reason} ->
            ?FAIL_MSG("Failed to parse config file ~s: ~p\n", [File, Reason])
    end.

set(Key, Value) ->
    ok = application:set_env(riak_bench, Key, Value).
    
get(Key) ->
    case application:get_env(riak_bench, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            erlang:error("Missing configuration key", [Key])
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

load_config([]) ->
    ok;
load_config([{Key, Value} | Rest]) ->
    ?MODULE:set(Key, Value),
    load_config(Rest);
load_config([ Other | Rest]) ->
    ?WARN("Ignoring non-tuple config value: ~p\n", [Other]),
    load_config(Rest).

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
-module(riak_bench_driver_innodb).

-export([new/1,
         run/4]).

-include("riak_bench.hrl").

-define(BUCKET, <<"bucket">>).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Make sure innodb app is available
    case code:which(riak_innodb_backend) of
        non_existing ->
            ?FAIL_MSG("~s requires riak_innodb_backend to be installed.\n", [?MODULE]);
        _ ->
            ok
    end,

    %% Pull the innodb_config key which has all the key/value pairs for the inno
    %% db engine -- stuff everything into the riak_innodb_backend application namespace
    %% so that starting the engine will pull them in
    InnoConfig = riak_bench_config:get(innodb_config, []),
    [ok = application:set_env(riak_innodb_backend, K, V) || {K, V} <- InnoConfig],
    
    riak_innodb_backend:start(0).


run(get, KeyGen, ValueGen, State) ->
    case riak_innodb_backend:get(State, {?BUCKET, KeyGen()}) of
        {ok, _Value} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    case riak_innodb_backend:put(State, {?BUCKET, KeyGen()}, ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end;
run(delete, KeyGen, ValueGen, State) ->
    case riak_innodb_backend:delete(State, {?BUCKET, KeyGen()}) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

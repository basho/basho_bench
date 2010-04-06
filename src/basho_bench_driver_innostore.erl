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
-module(basho_bench_driver_innostore).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Make sure innostore app is available
    case code:which(innostore) of
        non_existing ->
            ?FAIL_MSG("~s requires innostore to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    %% Pull the innodb_config key which has all the key/value pairs for the innostore
    %% engine -- stuff everything into the innostore application namespace
    %% so that starting innostore will pull it in.
    application:load(innostore),
    InnoConfig = basho_bench_config:get(innostore_config, []),
    [ok = application:set_env(innostore, K, V) || {K, V} <- InnoConfig],

    Bucket = basho_bench_config:get(innostore_bucket, <<"test">>),
    {ok, Port} = innostore:connect(),
    case innostore:open_keystore(Bucket, Port) of
        {ok, Store} ->
            {ok, Store};
        {error, Reason} ->
            ?FAIL_MSG("Failed to open keystore ~p: ~p\n", [Bucket, Reason])
    end.


run(get, KeyGen, _ValueGen, State) ->
    case innostore:get(KeyGen(), State) of
        {ok, not_found} ->
            {ok, State};
        {ok, _Value} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    case innostore:put(KeyGen(), ValueGen(), State) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    case innostore:delete(KeyGen(), State) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

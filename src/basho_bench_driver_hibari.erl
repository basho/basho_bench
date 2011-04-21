%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2011 Gemini Mobile Technologies, Inc.  All rights reserved.
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
-module(basho_bench_driver_hibari).

-export([init/0,
         new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { client,
                 table,
                 proto }).

%% ====================================================================
%% API
%% ====================================================================

init() ->
    %% Try to spin up net_kernel
    MyNode  = basho_bench_config:get(hibari_mynode, [basho_bench, shortnames]),
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, {{already_started, _}}, _} ->
            %% TODO: doesn't match documentation
            ok;
        {error, Reason1} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason1])
    end,

    %% Try to initialize the protocol-specific implementation
    Proto = basho_bench_config:get(hibari_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibari_table, tab1),
    init(Proto, Table).

new(_Id) ->
    Proto = basho_bench_config:get(hibari_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibari_table, tab1),

    %% Get a client
    case Proto of
        brick_simple_stub ->
            {ok, #state { client = brick_simple,
                          table = Table,
                          proto = Proto }};
        _ ->
            Reason1 = Proto,
            ?FAIL_MSG("Failed to get a hibari client: ~p\n", [Reason1])
    end.

run(get, KeyGen, _ValGen, State) ->
    Key = KeyGen(),
    case (State#state.client):get(State#state.table, Key) of
        {ok, _TS, _Val} ->
            {ok, State};
        key_not_exist ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValGen, State) ->
    Key = KeyGen(),
    Value = ValGen(),
    case (State#state.client):set(State#state.table, Key, Value) of
        ok ->
            {ok, State};
        Reason ->
            {error, Reason, State}
    end;
run(delete, KeyGen, _ValGen, State) ->
    Key = KeyGen(),
    case (State#state.client):delete(State#state.table, Key) of
        ok ->
            {ok, State};
        key_not_exist ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

init(brick_simple_stub=Proto, Table) ->
    %% Make sure the path is setup such that we can get at brick_simple_stub
    case code:which(Proto) of
        non_existing ->
            ?FAIL_MSG("~p requires ~p module to be available on code path.\n",
                      [?MODULE, Proto]);
        _ ->
            ok
    end,

    %% Make sure gmt_util is running
    case application:start(gmt_util) of
        ok ->
            ok;
        {error, {already_started,gmt_util}} ->
            ok;
        {error, Reason1} ->
            ?FAIL_MSG("Failed to start gmt_util for ~p: ~p\n", [?MODULE, Reason1])
    end,

    %% Make sure gdss_client is running
    case application:start(gdss_client) of
        ok ->
            ok;
        {error, {already_started,gdss_client}} ->
            ok;
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start gdss_client for ~p: ~p\n", [?MODULE, Reason2])
    end,

    ok = Proto:start(),
    ok = Proto:start_table(Table),
    Proto:wait_for_table(Table);
init(Proto, _Table) ->
    ?FAIL_MSG("Unknown protocol for ~p: ~p\n", [?MODULE, Proto]).

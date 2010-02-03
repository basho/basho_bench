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
-module(riak_bench_driver_riakclient).

-export([new/1,
         run/4]).

-include("riak_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riak_client) of
        non_existing ->
            ?FAIL_MSG("~s requires riak_client module to be available on code path.\n", [?MODULE]);
        _ ->
            ok
    end,

    Nodes = riak_bench_config:get(riakclient_nodes),
    Cookie = riak_bench_config:get(riakclient_cookie, 'riak'),
    MyNode = riak_bench_config:get(riakclient_mynode, [riak_bench, longnames]),

    %% Try to spin up net_kernel
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    %% Initialize cookie for each of the nodes
    [true = erlang:set_cookie(N, Cookie) || N <- Nodes],

    %% Try to ping each of the nodes
    ping_each(Nodes),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth(Nodes, (Id+1) rem length(Nodes)),
    ?INFO_MSG("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    case riak:client_connect(TargetNode) of
        {ok, Client} ->
            {ok, Client};
        {error, Reason} ->
            ?FAIL_MSG("Failed get a riak:client_connect to ~p: ~p\n", [TargetNode, Reason])
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case dets:lookup(?MODULE, Key) of
        [] ->
            {ok, State};
        [{Key, _}] ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    ok = dets:insert(?MODULE, {KeyGen(), ValueGen()}),
    {ok, State};
run(delete, KeyGen, _ValueGen, State) ->
    ok = dets:delete(?MODULE, KeyGen()),
    {ok, State}.




%% ====================================================================
%% Internal functions
%% ====================================================================

ping_each([]) ->
    ok;
ping_each([Node | Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            ping_each(Rest);
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.

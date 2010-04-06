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
-module(basho_bench_driver_riakclient).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { client,
                 bucket,
                 replies }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riak_client) of
        non_existing ->
            ?FAIL_MSG("~s requires riak_client module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Nodes   = basho_bench_config:get(riakclient_nodes),
    Cookie  = basho_bench_config:get(riakclient_cookie, 'riak'),
    MyNode  = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    Replies = basho_bench_config:get(riakclient_replies, 2),
    Bucket  = basho_bench_config:get(riakclient_bucket, <<"test">>),

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
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    case riak:client_connect(TargetNode) of
        {ok, Client} ->
            {ok, #state { client = Client,
                          bucket = Bucket,
                          replies = Replies }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed get a riak:client_connect to ~p: ~p\n", [TargetNode, Reason2])
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case (State#state.client):get(State#state.bucket, Key, State#state.replies) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Robj = riak_object:new(State#state.bucket, KeyGen(), ValueGen()),
    case (State#state.client):put(Robj, State#state.replies) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case (State#state.client):get(State#state.bucket, Key, State#state.replies) of
        {ok, Robj} ->
            Robj2 = riak_object:update_value(Robj, ValueGen()),
            case (State#state.client):put(Robj2, State#state.replies) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj = riak_object:new(State#state.bucket, Key, ValueGen()),
            case (State#state.client):put(Robj, State#state.replies) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;
run(delete, KeyGen, _ValueGen, State) ->
    case (State#state.client):delete(State#state.bucket, KeyGen(), State#state.replies) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


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

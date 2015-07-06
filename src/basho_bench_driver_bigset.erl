%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2015 Basho Techonologies
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
%% @doc copy of the basho_bench_driver_riakclient. Uses the internal
%% node client for bigsets. Still prototype code.
%% @end

-module(basho_bench_driver_bigset).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { client,
                 remove_set, %% The set name to perform a remove on
                 remove_ctx, %% The context of a get from `remove_set'
                 remove_values %% The values from a get to `remove_set'
               }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(bigset_client) of
        non_existing ->
            ?FAIL_MSG("~s requires bigset_client module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Nodes   = basho_bench_config:get(bigset_nodes),
    Cookie  = basho_bench_config:get(bigset_cookie, 'bigset'),
    MyNode  = basho_bench_config:get(bigset_mynode, [basho_bench, longnames]),

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

    case bigset_client:new(TargetNode) of
        {error, Reason2} ->
            ?FAIL_MSG("Failed get a bigset_client to ~p: ~p\n", [TargetNode, Reason2]);
        Client ->
            {ok, #state { client = Client }}
    end.

run(read, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    #state{client=C} = State,
    case bigset_client:read(Key, [], C) of
        {ok, {ctx, Ctx}, {elems, Set}} ->
            %% Store the latest Ctx/State for a remove
            {ok, State#state{remove_set=Key, remove_ctx=Ctx, remove_values=Set}};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(insert, KeyGen, ValueGen, State) ->
    #state{client=C} = State,
    Member = ValueGen(),
    Set = KeyGen(),
    case bigset_client:update(Set, [Member], C) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(remove, _KeyGen, _ValueGen, State) ->
    #state{client=C, remove_set=Key, remove_ctx=Ctx, remove_values=Vals} = State,
    RemoveVal = random_element(Vals),
    case bigset_client:update(Key, [], [RemoveVal], [{ctx, Ctx}], C) of
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

random_element(Vals) ->
    Nth = crypto:rand_uniform(1, length(Vals)),
    lists:nth(Nth, Vals).

ping_each([]) ->
    ok;
ping_each([Node | Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            ping_each(Rest);
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.

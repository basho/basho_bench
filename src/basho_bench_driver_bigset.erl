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
         run/4,
         worker_id_to_bin/1
        ]).

-include("basho_bench.hrl").

-record(state, { client,
                 last_key=undefined,
                 remove_set, %% The set name to perform a remove on
                 remove_ctx, %% The context of a get from `remove_set'
                 remove_value, %% The values from a get to `remove_set'
                 batch_size %% how many adds in a batch
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
    BatchSize = basho_bench_config:get(bigset_batchsize, 1000),

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
            {ok, #state { client = Client, batch_size=BatchSize }}
    end.

run(read, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    #state{client=C} = State,
    ?DEBUG("Read ~p~n", [Key]),
    case bigset_client:read(Key, [], C) of
        {ok, {ctx, Ctx}, {elems, Set}} ->
            %% Store the latest Ctx/State for a remove
            RemoveVal = random_element(Set),
            {ok, State#state{remove_set=Key, remove_ctx=Ctx, remove_value=RemoveVal}};
        {error, not_found} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(insert, KeyGen, ValueGen, State) ->
    #state{client=C, last_key=LastKey0} = State,
    {Member, LastKey} = try
                            {ValueGen(), LastKey0}
                        catch
                            throw:{stop, empty_keygen} ->
                                ?DEBUG("Empty keygen, reset~n", []),
                                basho_bench_keygen:reset_sequential_int_state(),
                                {ValueGen(), undefined}
                        end,

    Set = case LastKey of
              undefined ->
                  Key = KeyGen(),
                  ?DEBUG("New set ~p~n", [Key]),
                  Key;
              Bin when is_binary(Bin) ->
                  Bin
          end,
    State2 = State#state{last_key=Set},
    case bigset_client:update(Set, [Member], C) of
        ok ->
            {ok, State2};
        {error, Reason} ->
            {error, Reason, State2}
    end;
run(batch_insert, KeyGen, ValueGen, State) ->
    #state{client=C, batch_size=BatchSize, last_key=LastKey0} = State,

    {Set, Members} = case {LastKey0, gen_members(BatchSize, ValueGen)} of
                         {_, []} ->
                             %% Exhausted value gen, new key
                             Key = KeyGen(),
                             ?DEBUG("New set ~p~n", [Key]),
                             basho_bench_keygen:reset_sequential_int_state(),
                             {Key, gen_members(BatchSize, ValueGen)};
                         {undefined, List} ->
                             %% We have no active set, so generate a
                             %% key. First run maybe
                             Key = KeyGen(),
                             ?DEBUG("New set ~p~n", [Key]),
                             {Key, List};
                         Else ->
                             Else
                     end,

    State2 = State#state{last_key=Set},

    case bigset_client:update(Set, Members, C) of
        ok ->
            {ok, State2};
        {error, Reason} ->
            {error, Reason, State2}
    end;
run(remove, _KeyGen, _ValueGen, State) ->
    #state{client=C, remove_set=Key, remove_ctx=Ctx, remove_value=RemoveVal} = State,
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

%% @private generate as many elements as we can from the valgen, if it
%% exhausts, return the results we did get.
gen_members(BatchSize, ValueGen) ->
    accumulate_members(BatchSize, ValueGen, []).

%% @private generate as many elements as we can from the valgen, if it
%% exhausts, return the results we did get.
accumulate_members(0, _ValueGen, Acc) ->
    lists:reverse(Acc);
accumulate_members(BS, Gen, Acc) ->
    try
        accumulate_members(BS-1, Gen, [Gen() | Acc])
    catch throw:{stop, empty_keygen} ->
            ?DEBUG("ValGen exhausted~n", []),
            lists:reverse(Acc)
    end.

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

%% ====================================================================
%% Generators
%% ====================================================================

worker_id_to_bin(Id) when is_integer(Id) ->
    fun() ->
            <<Id:32/big>>
    end;
worker_id_to_bin(Id) ->
    BINID = term_to_binary(Id),
    fun() ->
            BINID
    end.

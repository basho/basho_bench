%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking suite for riak datatype sets using the
%%              localclient.
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
%% node client for dt sets (for comparision to basho_bench_driver_bigset)
%% @end

-module(basho_bench_driver_riakclient_sets).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-define(DEFAULT_SET_KEY, <<"bench_set">>).
-define(SET_TYPE, riak_dt_orswot).

-record(state, { client,
                 bucket,
                 last_key=undefined,
                 set_val_gen_name = undefined,
                 remove_set, %% The set name to perform a remove on
                 remove_ctx, %% The context of a get from `remove_set'
                 remove_value, %% a value from a get to `remove_set'
                 batch_size, %% in batch inserts, how many at once
                 preload,
                 preloaded_sets,
                 preloaded_sets_num,
                 var_bin_size,
                 last_preload_nth,
                 max_vals_for_preload,
                 run_one_set
               }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riak_client) of
        non_existing ->
            ?FAIL_MSG("~s requires riak_client module to be available on" ++
                          " code path.\n", [?MODULE]);
        _ ->
            ok
    end,

    Nodes   = basho_bench_config:get(riakclient_nodes),
    Cookie  = basho_bench_config:get(riak_cookie, 'riak'),
    MyNode  = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    Bucket  = basho_bench_config:get(riakclient_bucket, {<<"sets">>, <<"test">>}),
    BatchSize = basho_bench_config:get(riakclient_sets_batchsize, 1000),
    SetValGenName = basho_bench_config:get(riakclient_sets_valgen_name, undefined),
    Preload = basho_bench_config:get(riakclient_preload_sets, false),
    PreloadNum = basho_bench_config:get(riakclient_preload_sets_num, 10),
    MaxValsForPreloadSet = basho_bench_config:get(
                             riakclient_max_vals_for_preload, 100),
    RunOneSet = basho_bench_config:get(riakclient_run_one_set, false),

    ValueGenTup = basho_bench_config:get(value_generator),
    StartBinSize = basho_bench_riak_dt_util:set_start_bin_size(ValueGenTup),

    PreloadedSets = if Preload ->
                            [begin X1 = integer_to_binary(X),
                                   Y = <<"set">>,
                                   <<X1/binary,Y/binary>>
                             end || X <- lists:seq(1, PreloadNum)];
                       true -> []
                    end,

    %% Try to spin up net_kernel
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n",
                      [?MODULE, Reason])
    end,

    %% Initialize cookie for each of the nodes
    [true = erlang:set_cookie(N, Cookie) || N <- Nodes],

    %% Try to ping each of the nodes
    ping_each(Nodes),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    case riak:client_connect(TargetNode) of
        {error, Reason2} ->
            ?FAIL_MSG("Failed get a riak_client to ~p: ~p\n",
                      [TargetNode, Reason2]);
        {ok, Client} ->
            case {Preload, RunOneSet} of
                {true, _} ->
                    preload_sets(Client, PreloadedSets, Bucket,
                                 StartBinSize);
                {_, true} ->
                    run_one_set(Client, Bucket, StartBinSize);
                {_, _} ->
                    lager:info("No special pre-operations specified.")
            end,
            {ok, #state {client=Client,
                         bucket=Bucket,
                         batch_size=BatchSize,
                         set_val_gen_name=SetValGenName,
                         preload = Preload,
                         preloaded_sets = PreloadedSets,
                         preloaded_sets_num = PreloadNum,
                         last_preload_nth = 0,
                         max_vals_for_preload = MaxValsForPreloadSet,
                         run_one_set = RunOneSet
                        }}
    end.

run(read, _KeyGen, _ValueGen, #state{run_one_set=true}=State) ->
    read_set(?DEFAULT_SET_KEY, State);
run(read, _KeyGen, _ValueGen,
    #state{preload=true,
           preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum}=State) ->
    Key = lists:nth(random:uniform(PreloadedSetsNum), PreloadedSets),
    read_set(Key, State);
run(read, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    read_set(Key, State);

run(insert_no_ctx, _KeyGen, ValueGen, #state{run_one_set=true}=State) ->
    Member = ValueGen(),
    add_element(?DEFAULT_SET_KEY, Member, State);
run(insert_no_ctx, KeyGen, ValueGen, State) ->
    run(insert, KeyGen, ValueGen, State);

run(insert, _KeyGen, ValueGen, #state{run_one_set=true,
                                      client=C, bucket=B}=State) ->
    case C:get(B, ?DEFAULT_SET_KEY, []) of
        {ok, Res} ->
            Member = ValueGen(),
            {{Ctx, _Values}, _Stats} = riak_kv_crdt:value(Res, ?SET_TYPE),
            add_element(?DEFAULT_SET_KEY, Member, Ctx, State);
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(insert, _KeyGen, ValueGen,
    #state{client=C, bucket=B, preload=true,
                  preloaded_sets=PreloadedSets,
                  preloaded_sets_num=PreloadedSetsNum,
                  last_preload_nth=LastNth,
                  max_vals_for_preload=MaxValsForPreloadSet}=State) ->
    NextNth = case LastNth >= PreloadedSetsNum of
                  true -> 1;
                  false -> LastNth + 1
              end,
    Key = lists:nth(NextNth, PreloadedSets),
    case C:get(B, Key, []) of
        {ok, Res} ->
            Member = ValueGen(),
            {{Ctx, Values}, _Stats} = riak_kv_crdt:value(Res, ?SET_TYPE),
            SetSize = length(Values),
            if SetSize < MaxValsForPreloadSet ->
                    add_element(Key, Member, Ctx, State);
               true -> {ok, State#state{last_preload_nth=NextNth}}
            end;
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, {Reason, Key}, State}
    end;
run(insert, KeyGen, ValueGen, State) ->
    Member = ValueGen(),
    Set = KeyGen(),
    add_element(Set, Member, State);

run(insert_pl, KeyGen, ValueGen, State) ->
    #state{last_key=LastKey0} = State,
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
    add_element(Set, Member, State2);

run(batch_insert, KeyGen, ValueGen, State) ->
    #state{client=C, bucket=B, batch_size=BatchSize,
           last_key=LastKey,
           set_val_gen_name=SVGN} = State,
    {Set, Members} = basho_bench_riak_dt_util:gen_set_batch(KeyGen,
                                                            ValueGen,
                                                            LastKey,
                                                            BatchSize,
                                                            SVGN),
    State2 = State#state{last_key=Set},

    O = riak_kv_crdt:new(B, Set, ?SET_TYPE),
    Opp = riak_kv_crdt:operation(?SET_TYPE, {add_all, Members}, undefined),
    Options1 = [{crdt_op, Opp}],
    case C:put(O, Options1) of
        ok ->
            {ok, State2};
        {error, Reason} ->
            {error, Reason, State2}
    end;

run(remove, _KeyGen, ValueGen, #state{run_one_set=true}=State) ->
    Member= ValueGen(),
    remove_element(?DEFAULT_SET_KEY, Member, State);
run(remove, _KeyGen, ValueGen,
    #state{preload=true,
           preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum}=State) ->
    Key = lists:nth(random:uniform(PreloadedSetsNum), PreloadedSets),
    Member = ValueGen(),
    remove_element(Key, Member, State);
run(remove, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Member = ValueGen(),
    remove_element(Key, Member, State);

run(remove_last_read, KeyGen, ValueGen, #state{remove_set=undefined}=State) ->
    Key = KeyGen(),
    Member = ValueGen(),
    remove_element(Key, ValueGen, State);
run(remove_last_read, _KeyGen, _ValueGen, State) ->
    #state{remove_set=Key, remove_ctx=Ctx,
           remove_value=RemoveVal}=State,
    remove_element(Key, RemoveVal, Ctx, State).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private preload and update riak with an N-number of set keys,
%% named in range <<"1..Nset">>.
preload_sets(C, PreloadKeys, Bucket, StartBinSize) ->
    [begin
         case C:get(Bucket, ?DEFAULT_SET_KEY, []) of
             {ok, _} ->
                 ?INFO("~p Already Loaded", [SetKey]),
                 SetKey;
             {error, notfound} ->
                 case add_element(SetKey,
                                  crypto:rand_bytes(StartBinSize),
                                  #state{client=C, bucket=Bucket}) of
                     {ok, _} ->
                         ?INFO("~p created", [SetKey]),
                         SetKey;
                     {error, Reason} ->
                         ?ERROR("~p not created b/c ~p", [SetKey,
                                                          Reason]),
                         error
                 end;
             {error, Reason} ->
                 ?ERROR("~p not created b/c ~p", [SetKey, Reason]),
                 SetKey
         end
     end || SetKey <- PreloadKeys].

run_one_set(C, Bucket, StartBinSize) ->
    case C:get(Bucket, ?DEFAULT_SET_KEY, []) of
        {ok, _} ->
            ?INFO("~p Already Loaded", [?DEFAULT_SET_KEY]);
        {error, notfound} ->
            case add_element(?DEFAULT_SET_KEY, crypto:rand_bytes(StartBinSize),
                             #state{client=C, bucket=Bucket}) of
                {ok, _} ->
                    ?INFO("~p created", [?DEFAULT_SET_KEY]);
                {error, Reason} ->
                    ?ERROR("~p not created b/c ~p", [?DEFAULT_SET_KEY, Reason])
            end;
        {error, Reason} ->
            ?ERROR("~p not created b/c ~p", [?DEFAULT_SET_KEY, Reason])
    end.

ping_each([]) ->
    ok;
ping_each([Node | Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            ping_each(Rest);
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.

add_element(Key, Val, State) ->
    add_element(Key, Val, undefined, State).
add_element(Key, Val, Ctx, #state{client=C, bucket=B}=State) ->
    O = riak_kv_crdt:new(B, Key, ?SET_TYPE),
    Opp = riak_kv_crdt:operation(?SET_TYPE, {add, Val}, Ctx),
    Options1 = [{crdt_op, Opp}],
    case C:put(O, Options1) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

remove_element(Key, Val, #state{client=C, bucket=B}=State) ->
    %% Force getting a context for remove.
    case C:get(B, Key, []) of
        {ok, Res} ->
            {{Ctx, _Values}, _Stats} = riak_kv_crdt:value(Res, ?SET_TYPE),
            remove_element(Key, Val, Ctx, State);
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.
remove_element(Key, Val, undefined, State) ->
    remove_element(Key, Val, State);
remove_element(Key, Val, Ctx, #state{client=C, bucket=B}=State) ->
    O = riak_kv_crdt:new(B, Key, ?SET_TYPE),
    Opp = riak_kv_crdt:operation(?SET_TYPE, {remove, Val}, Ctx),
    Options1 = [{crdt_op, Opp}],
    case C:put(O, Options1) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

read_set(Key, #state{client=C, bucket=B}=State) ->
    case C:get(B, Key, []) of
        {ok, Res} ->
            {{Ctx, Values}, _Stats} = riak_kv_crdt:value(Res, ?SET_TYPE),
            RemoveVal = basho_bench_riak_dt_util:random_element(Values),
            %% Store the latest Ctx/State for a remove
            {ok, State#state{remove_set=Key,
                             remove_ctx=Ctx,
                             remove_value=RemoveVal}};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_dt_pb: Driver for riak protocol buffers client wrt
%%                                 to riak datatypes, specifically sets & maps
%%
%% Copyright (c) 2016 Basho Techonologies
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

-module(basho_bench_driver_riakc_dt_pb).

-export([new/1,
         run/4]).

%% Special exports for fun apply for multi-map ops
-export([map_counter/2,
         map_flag/2,
         map_register/2,
         map_set/2,
         map_map/2]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 last_key=undefined,
                 batch_size,
                 set_val_gen_name=undefined,
                 preload,
                 preloaded_sets,
                 preloaded_sets_num,
                 var_bin_size,
                 last_preload_nth,
                 max_vals_for_preload,
                 run_one_set,
                 run_one_map,
                 map_multi_ops
               }).

-define(DEFAULT_SET_KEY, <<"bench_set">>).
-define(DEFAULT_MAP_KEY, <<"bench_map">>).
-define(DEFAULT_MAP_OPS, [map_counter, map_register, map_flag, map_set,
                          map_map]).

new(Id) ->
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available" ++
                          "on code path.\n", [?MODULE]);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_dt_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_dt_port, 8087),
    Bucket  = basho_bench_config:get(riakc_dt_bucket, {<<"riak_dt">>,
                                                       <<"test">>}),
    BatchSize = basho_bench_config:get(riakc_dt_sets_batchsize, 100),
    SetValGenName = basho_bench_config:get(riakc_dt_preload_valgen_name,
                                           undefined),
    Preload = basho_bench_config:get(riakc_dt_preload_sets, false),
    PreloadNum = basho_bench_config:get(riakc_dt_preload_sets_num, 10),
    MaxValsForPreloadSet = basho_bench_config:get(
                             riakc_dt_max_vals_for_preload, 100),
    RunOneSet = basho_bench_config:get(riakc_dt_run_one_set, false),
    RunOneMap = basho_bench_config:get(riakc_dt_run_one_map, false),

    MapMultiOpsL = basho_bench_config:get(riakc_dt_map_multi_ops, []),
    MapMultiOps = case MapMultiOpsL of
                      [] -> [];
                      _ -> filter_map_multi_ops(MapMultiOpsL, ?DEFAULT_MAP_OPS)

                  end,

    ValueGenTup = basho_bench_config:get(value_generator),
    StartBinSize = basho_bench_riak_dt_util:set_start_bin_size(ValueGenTup),

    PreloadedSets = if Preload ->
                            [begin X1 = integer_to_binary(X),
                                   Y = <<"set">>,
                                   <<X1/binary,Y/binary>>
                             end || X <- lists:seq(1, PreloadNum)];
                       true -> []
                    end,

    %% Choose the target node using our ID as a modulus
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    lager:info("Ips: ~p Targets: ~p\n", [Ips, Targets]),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(
           TargetIp, TargetPort, get_connect_options()) of
        {ok, Pid} ->
            case {Preload, RunOneSet, RunOneMap} of
                {true, _, _} ->
                    preload_sets(Pid, PreloadedSets, Bucket,
                                 StartBinSize);
                {_, true, _} ->
                    run_one_set(Pid, Bucket, StartBinSize);
                {_, _, true} ->
                    run_one_map(Pid, Bucket);
                {_, _, _} ->
                    lager:info("No special pre-operations specified.")
            end,
            {ok, #state{ pid = Pid,
                         bucket = Bucket,
                         batch_size = BatchSize,
                         set_val_gen_name=SetValGenName,
                         preload = Preload,
                         preloaded_sets = PreloadedSets,
                         preloaded_sets_num = PreloadNum,
                         last_preload_nth = 0,
                         max_vals_for_preload = MaxValsForPreloadSet,
                         run_one_set = RunOneSet,
                         run_one_map = RunOneMap,
                         map_multi_ops = MapMultiOps
                       }};
        {error, Reason} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason])
    end.

%%%===================================================================
%%% Sets
%%%===================================================================\

run({set, insert_no_ctx}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                    run_one_set=true}=State) ->
    Val = ValueGen(),
    Set0 = riakc_set:new(),
    Set1 = riakc_set:add_element(Val, Set0),
    update_type(Pid, Bucket, ?DEFAULT_SET_KEY, {set, Set1}, State);
run({set, insert_no_ctx}, KeyGen, ValueGen, State) ->
    run({set, insert}, KeyGen, ValueGen, State);

run({set, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                             run_one_set=true}=State) ->
    case riakc_pb_socket:fetch_type(Pid, Bucket, ?DEFAULT_SET_KEY) of
        {ok, Set0} ->
            Val = ValueGen(),
            Set1 = riakc_set:add_element(Val, Set0),
            update_type(Pid, Bucket, ?DEFAULT_SET_KEY, {set, Set1}, State);
        {error, {notfound, _}} ->
            {ok, State};
        {error, FetchReason} ->
            {error, FetchReason, State}
    end;
run({set, insert}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, preload=true, preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum, last_preload_nth=LastNth,
           max_vals_for_preload=MaxValsForPreloadSet}=State) ->
    NextNth = case LastNth >= PreloadedSetsNum of
                  true -> 1;
                  false -> LastNth + 1
              end,
    SetKey = lists:nth(NextNth, PreloadedSets),
    case riakc_pb_socket:fetch_type(Pid, Bucket, SetKey) of
        {ok, Set0} ->
            Val = ValueGen(),
            SetSize = riakc_set:size(Set0),
            if SetSize < MaxValsForPreloadSet ->
                    Set1 = riakc_set:add_element(Val, Set0),
                    update_type(Pid, Bucket, SetKey, {set, Set1}, State,
                               State#state{last_preload_nth=NextNth});
               true -> {ok, State#state{last_preload_nth=NextNth}}
            end;
        {error, {notfound, _}} ->
            {ok, State};
        {error, FetchReason} ->
            {error, {FetchReason, SetKey}, State}
    end;
run({set, insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    Val = ValueGen(),
    Set0 = riakc_set:new(),
    Set1 = riakc_set:add_element(Val, Set0),
    update_type(Pid, Bucket, SetKey, {set, Set1}, State);

run({set, modify}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    Val = ValueGen(),
    SetFun = fun(S) -> riakc_set:add_element(Val, S) end,
    modify_type(Pid, Bucket, SetKey, SetFun, State);

%% Note: Make sure to not use sequential for keys when run, unless
%%       supplying a specific SetValGenName.
run({set, batch_insert}, KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, last_key=LastKey, batch_size=BatchSize,
           set_val_gen_name=SetValGenName}=State) ->
    {SetKey, Members} = basho_bench_riak_dt_util:gen_set_batch(KeyGen,
                                                               ValueGen,
                                                               LastKey,
                                                               BatchSize,
                                                               SetValGenName),
    Set0 = riakc_set:new(),
    SetLast = lists:foldl(fun(Elem, Sets) ->
                              Sets ++ [riakc_set:add_element(Elem, Set0)]
                         end, [], Members),
    State2 = State#state{last_key=SetKey},
    update_type(Pid, Bucket, SetKey, {set, lists:last(SetLast)}, State2);

run({set, read}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                           run_one_set=true}=State) ->
    fetch_action(Pid, Bucket, ?DEFAULT_SET_KEY, ValueGen, State, read);
run({set, read}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, preload=true, preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum}=State) ->
    SetKey = lists:nth(random:uniform(PreloadedSetsNum), PreloadedSets),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, read);
run({set, read}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, read);

run({set, remove}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                             run_one_set=true}=State) ->
    fetch_action(Pid, Bucket, ?DEFAULT_SET_KEY, ValueGen, State, remove_element);
run({set, remove}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, preload=true, preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum}=State) ->
    SetKey = lists:nth(random:uniform(PreloadedSetsNum), PreloadedSets),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, remove_element);
run({set, remove}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, remove_element);

run({set, is_element}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                 run_one_set=true}=State) ->
    fetch_action(Pid, Bucket, ?DEFAULT_SET_KEY, ValueGen, State, is_element);
run({set, is_element}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, preload=true, preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum}=State) ->
    SetKey = lists:nth(random:uniform(PreloadedSetsNum), PreloadedSets),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, is_element);
run({set, is_element}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, is_element);

%%%===================================================================
%%% Maps
%%%===================================================================

run({map, set, insert_no_ctx}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                         run_one_map=true}=State) ->
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWSet = map_set(Map, Val),
    update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWSet}, State);

run({map, set, insert_no_ctx}, KeyGen, ValueGen, State) ->
    run({map, set, insert}, KeyGen, ValueGen, State);

run({map, set, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                  run_one_map=true}=State) ->
    Val = ValueGen(),
    FetchedMap = fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State,
                              return_dt),
    case element(1, FetchedMap) of
        map ->
            MapWSet = map_set(FetchedMap, Val),
            update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWSet}, State);
        _ -> FetchedMap %% return ok or error state from fetch call
    end;

run({map, set, insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWSet = map_set(Map, Val),
    update_type(Pid, Bucket, MapKey, {map, MapWSet}, State);

run({map, set, modify}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    MapFun = fun(M) -> map_set(M, Val) end,
    modify_type(Pid, Bucket, MapKey, MapFun, State);

run({map, set, remove}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                  run_one_map=true}=State) ->
    Val = ValueGen(),
    FetchedMap = fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State,
                              return_dt),
    case element(1, FetchedMap) of
        map ->
            MapWSet = map_set_remove(FetchedMap, Val),
            update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWSet}, State);
        _ -> FetchedMap %% return ok or error state from fetch call
    end;

run({map, set, remove}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    MapFun = fun(M) -> map_set_remove(M, Val) end,
    modify_type(Pid, Bucket, MapKey, MapFun, State, [{create, false}]);

run({map, register, insert_no_ctx}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                              run_one_map=true}=State) ->
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWReg = map_register(Map, Val),
    update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWReg}, State);

run({map, register, insert_no_ctx}, KeyGen, ValueGen, State) ->
    run({map, register, insert}, KeyGen, ValueGen, State);

run({map, register, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                       run_one_map=true}=State) ->
    Val = ValueGen(),
    FetchedMap = fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State,
                              return_dt),
    case element(1, FetchedMap) of
        map ->
            MapWReg = map_register(FetchedMap, Val),
            update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWReg}, State);
        _ -> FetchedMap %% return ok or error state from fetch call
    end;

run({map, register, insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWReg = map_register(Map, Val),
    update_type(Pid, Bucket, MapKey, {map, MapWReg}, State);

run({map, register, modify}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    MapFun = fun(M) -> map_register(M, Val) end,
    modify_type(Pid, Bucket, MapKey, MapFun, State);

run({map, flag, insert_no_ctx}, _KeyGen, _ValueGen, #state{pid=Pid, bucket=Bucket,
                                                           run_one_map=true}=State) ->
    Map = riakc_map:new(),
    MapWFlag = map_flag(Map),
    update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWFlag}, State);

run({map, flag, insert_no_ctx}, KeyGen, ValueGen, State) ->
    run({map, flag, insert}, KeyGen, ValueGen, State);

run({map, flag, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                   run_one_map=true}=State) ->
    FetchedMap = fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State,
                              return_dt),
    case element(1, FetchedMap) of
        map ->
            MapWFlag = map_flag(FetchedMap),
            update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWFlag}, State);
        _ -> FetchedMap %% return ok or error state from fetch call
    end;

run({map, flag, insert}, KeyGen, _ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Map = riakc_map:new(),
    MapWFlag = map_flag(Map),
    update_type(Pid, Bucket, MapKey, {map, MapWFlag}, State);

run({map, flag, modify}, KeyGen, _ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    MapFun = fun(M) -> map_flag(M) end,
    modify_type(Pid, Bucket, MapKey, MapFun, State);

run({map, counter, insert_no_ctx}, _KeyGen, _ValueGen, #state{pid=Pid, bucket=Bucket,
                                                              run_one_map=true}=State) ->
    Map = riakc_map:new(),
    MapWCnt = map_counter(Map),
    update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWCnt}, State);

run({map, counter, insert_no_ctx}, KeyGen, ValueGen, State) ->
    run({map, counter, insert}, KeyGen, ValueGen, State);

run({map, counter, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                      run_one_map=true}=State) ->
    FetchedMap = fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State,
                              return_dt),
    case element(1, FetchedMap) of
        map ->
            MapWCnt = map_counter(FetchedMap),
            update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWCnt}, State);
        _ ->
            FetchedMap %% return ok or error state from fetch call
    end;

run({map, counter, insert}, KeyGen, _ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Map = riakc_map:new(),
    MapWCnt = map_counter(Map),
    update_type(Pid, Bucket, MapKey, {map, MapWCnt}, State);

run({map, counter, modify}, KeyGen, _ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    MapFun = fun(M) -> map_counter(M) end,
    modify_type(Pid, Bucket, MapKey, MapFun, State);

run({map, map, insert_no_ctx}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                         run_one_map=true}=State) ->
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWMap = map_map(Map, Val),
    update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWMap}, State);

run({map, map, insert_no_ctx}, KeyGen, ValueGen, State) ->
    run({map, map, insert}, KeyGen, ValueGen, State);

run({map, map, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                  run_one_map=true}=State) ->
    Val = ValueGen(),
    FetchedMap = fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State,
                              return_dt),
    case element(1, FetchedMap) of
        map ->
            MapWMap = map_map(FetchedMap, Val),
            update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWMap}, State);
        _ -> FetchedMap %% return ok or error state from fetch call
    end;

run({map, map, insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWMap = map_map(Map, Val),
    update_type(Pid, Bucket, MapKey, {map, MapWMap}, State);

run({map, map, modify}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    MapFun = fun(M) -> map_map(M, Val) end,
    modify_type(Pid, Bucket, MapKey, MapFun, State);

run({map, multi_ops, insert_no_ctx}, KeyGen, ValueGen, #state{map_multi_ops=[]}=State) ->
    run({map, multi_ops, insert}, KeyGen, ValueGen, State);

run({map, multi_ops, insert_no_ctx}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, run_one_map=true, map_multi_ops=Ops}=State) ->
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWOps = map_multi(Map, Val, Ops),
    update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWOps}, State);

run({map, multi_ops, insert_no_ctx}, KeyGen, ValueGen, State) ->
    run({map, multi_ops, insert}, KeyGen, ValueGen, State);

run({map, multi_ops, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                        map_multi_ops=[],
                                                        run_one_map=true}=State) ->
    %% If No Ops, treat as read
    fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State, read);

run({map, multi_ops, insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                       map_multi_ops=[]}=State) ->
    %% If No Ops, treat as read
    MapKey = KeyGen(),
    fetch_action(Pid, Bucket, MapKey, ValueGen, State, read);

run({map, multi_ops, insert}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket,
               map_multi_ops=Ops,
               run_one_map=true}=State) ->
    Val = ValueGen(),
    FetchedMap = fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State,
                              return_dt),
    case element(1, FetchedMap) of
        map ->
            MapWOps = map_multi(FetchedMap, Val, Ops),
            update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, MapWOps}, State);
        _ -> FetchedMap %% return ok or error state from fetch call
    end;

run({map, multi_ops, insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                       map_multi_ops=Ops}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    Map = riakc_map:new(),
    MapWOps = map_multi(Map, Val, Ops),
    update_type(Pid, Bucket, MapKey, {map, MapWOps}, State);

run({map, multi_ops, modify}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                           map_multi_ops=Ops}=State) ->
    MapKey = KeyGen(),
    Val = ValueGen(),
    MapFun = fun(M) -> map_multi(M, Val, Ops) end,
    modify_type(Pid, Bucket, MapKey, MapFun, State);

run({map, read}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                           run_one_map=true}=State) ->
    fetch_action(Pid, Bucket, ?DEFAULT_MAP_KEY, ValueGen, State, read);

run({map, read}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    MapKey = KeyGen(),
    fetch_action(Pid, Bucket, MapKey, ValueGen, State, read).


%%%===================================================================
%%% Private
%%%===================================================================

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).

%% @private preload and update riak with an N-number of set keys,
%% named in range <<"1..Nset">>.
preload_sets(Pid, PreloadKeys, Bucket, StartBinSize) ->
    [begin
         case riakc_pb_socket:fetch_type(Pid, Bucket, SetKey) of
             {error, _} ->
                 Set0 = riakc_set:new(),
                 Set1 = riakc_set:add_element(crypto:rand_bytes(StartBinSize),
                                              Set0),
                 case update_type(Pid, Bucket, SetKey, {set, Set1}) of
                     {ok, _} ->
                         ?INFO("~p created", [SetKey]),
                         SetKey;
                     {error, Reason} ->
                         ?ERROR("~p not created b/c ~p", [SetKey, Reason]),
                         error
                 end;
             _ ->
                 ?INFO("~p Already Loaded", [SetKey]),
                 SetKey
         end
     end || SetKey <- PreloadKeys].

%% @private check by fetching single-set run key before creating a new one.
run_one_set(Pid, Bucket, StartBinSize) ->
    case riakc_pb_socket:fetch_type(Pid, Bucket, ?DEFAULT_SET_KEY) of
        {error, _} ->
            Set0 = riakc_set:new(),

            %% can't be unmodified
            Set1 = riakc_set:add_element(crypto:rand_bytes(StartBinSize), Set0),
            case update_type(Pid, Bucket, ?DEFAULT_SET_KEY, {set, Set1}) of
                {ok, _} ->
                    ?INFO("~p created", [?DEFAULT_SET_KEY]);
                {error, Reason} ->
                    ?ERROR("~p not created b/c ~p", [?DEFAULT_SET_KEY, Reason])
            end;
        _ ->
            ?INFO("~p Already Loaded", [?DEFAULT_SET_KEY])
    end.

%% @private check by fetching single-map run key before creating a new one.
run_one_map(Pid, Bucket) ->
    case riakc_pb_socket:fetch_type(Pid, Bucket, ?DEFAULT_MAP_KEY) of
        {error, _} ->
            Map0 = riakc_map:new(),

            %% can't be unmodified
            Map1 = map_counter(Map0),
            case update_type(Pid, Bucket, ?DEFAULT_MAP_KEY, {map, Map1}) of
                {ok, _} ->
                    ?INFO("~p created", [?DEFAULT_MAP_KEY]);
                {error, Reason} ->
                    ?ERROR("~p not created b/c ~p", [?DEFAULT_MAP_KEY, Reason])
            end;
        _ ->
            ?INFO("~p Already Loaded", [?DEFAULT_MAP_KEY])
    end.

%% @private fetch helper for read, remove, is_element runs
fetch_action(Pid, Bucket, Key, ValueGen, State, Action) ->
    case riakc_pb_socket:fetch_type(Pid, Bucket, Key) of
        {ok, DT0} ->
            %% If wanting to check if elem is a member of the set first
            %%   - M = riakc_map:value(M0),
            %%   - Members = proplists:get_value({<<"members">>, set}, M, []),
            %%   - case {Action, length(Members) > 0} of ...
            case Action of
                remove_element ->
                    Val = ValueGen(),
                    DT1 = riakc_set:del_element(Val, DT0),
                    update_type(Pid, Bucket, Key, {set, DT1}, State);
                is_element ->
                    Val = ValueGen(),
                    riakc_set:is_element(Val, DT0),
                    {ok, State};
                return_dt ->
                    DT0;
                _ ->
                    {ok, State}
            end;
        {error, {notfound, _}} ->
            {ok, State};
        {error, Reason} ->
            {error, {Reason, Key}, State}
    end.

%% @private riac_pb_socket:modify_type wrapper
modify_type(Pid, Bucket, _Key, Fun, #state{run_one_set=true}=State) ->
    modify_type(Pid, Bucket, ?DEFAULT_SET_KEY, Fun, State, State);
modify_type(Pid, Bucket, _Key, Fun, #state{run_one_map=true}=State) ->
    modify_type(Pid, Bucket, ?DEFAULT_MAP_KEY, Fun, State, State);
modify_type(Pid, Bucket, Key, Fun, State) ->
    modify_type(Pid, Bucket, Key, Fun, State, State).

modify_type(Pid, Bucket, Key, Fun, State, Options) when is_list(Options) ->
    modify_type(Pid, Bucket, Key, Fun, State, State, Options);
modify_type(Pid, Bucket, Key, Fun, State, State) ->
    modify_type(Pid, Bucket, Key, Fun, State, State, [{create, true}]).

modify_type(Pid, Bucket, Key, Fun, StateOnError, StateOnUpdate, Options) ->
    case riakc_pb_socket:modify_type(Pid, Fun, Bucket, Key, Options) of
        ok ->
            {ok, StateOnUpdate};
        {ok, _} ->
            {ok, StateOnUpdate};
        {ok, _, _} ->
            {ok, StateOnUpdate};
        {error, {notfound, _}} ->
            {ok, StateOnError};
        {error, Reason} ->
            {error, {Reason, Key, modify_type}, StateOnError}
    end.

%% @private riac_pb_socket:update_type wrapper
update_type(Pid, Bucket, Key, ToOp) ->
    update_type(Pid, Bucket, Key, ToOp, no_state).
update_type(Pid, Bucket, Key, ToOp, State) ->
    update_type(Pid, Bucket, Key, ToOp, State, State).
update_type(Pid, Bucket, Key, ToOp, StateOnError, StateOnUpdate) ->
    case riakc_pb_socket:update_type(Pid, Bucket, Key, to_op(ToOp)) of
        ok ->
            {ok, StateOnUpdate};
        {ok, _} ->
            {ok, StateOnUpdate};
        {ok, _, _} ->
            {ok, StateOnUpdate};
        {error, Reason} ->
            {error, {Reason, Key, update_type}, StateOnError}
    end.

%% @private Case-specific for Op
to_op({set, ToOp}) ->
    riakc_set:to_op(ToOp);
to_op({map, ToOp}) ->
    riakc_map:to_op(ToOp).

%% @private Helpers for map operations

map_counter(Map, _) ->
    map_counter(Map).
map_counter(Map) ->
    riakc_map:update(
      {<<"69">>, counter},
      fun(C) ->
              riakc_counter:increment(C)
      end, Map).

map_flag(Map, _) ->
    map_flag(Map).
map_flag(Map) ->
    riakc_map:update(
      {<<"familymattersandfullhouses">>, flag},
      fun(F) ->
              riakc_flag:enable(F)
      end, Map).

map_register(Map, Val) ->
    riakc_map:update(
      {<<"soup">>, register},
      fun(R) ->
              riakc_register:set(Val, R)
      end, Map).

map_set(Map, Val) ->
    riakc_map:update(
      {<<"justbirdtome">>, set},
      fun(S) ->
              riakc_set:add_element(Val, S)
      end, Map).

map_set_remove(Map, Val) ->
    riakc_map:update(
      {<<"justbirdtome">>, set},
      fun(S) ->
              riakc_set:del_element(Val, S)
      end, Map).

map_map(Map, Val) ->
    riakc_map:update(
      {<<"bad hobbits die hard">>, map},
      %% start by adding a register to the nested map
      fun(M) ->
              map_register(M, Val)
      end, Map).

%% @doc Update top-level map w/ through-a/with-a list of operations before
%%      sending the final map (of updates) to the server
map_multi(Map, Val, Ops) ->
    lists:foldl(fun(F, AccM) ->
                        apply(basho_bench_driver_riakc_dt_pb, F, [AccM, Val])
                end, Map, Ops).

filter_map_multi_ops(Ops, PossibleOps) ->
    OpsS = sets:from_list(lists:filter(fun(Elem) -> lists:member(Elem, PossibleOps)
                                       end, Ops)),
    case length(Ops) == sets:size(OpsS) of
        true ->
            lager:info("Using all map operations ~p in a single update", [Ops]),
            Ops;
        false ->
            UList = sets:to_list(OpsS),
            lager:info("Using only unique/working map operations ~p in a single update",
                       [UList]),
            UList
    end.

% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_dt_pb: Driver for riak protocol buffers client wrt
%%                                 to riak datatypes
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

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 last_key,
                 batch_size,
                 preload,
                 preloaded_sets,
                 preloaded_sets_num,
                 var_bin_size,
                 last_preload_nth,
                 max_vals_for_preload,
                 run_one_set
               }).

-define(DEFAULT_SET_KEY, <<"bench_set">>).

new(Id) ->
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_dt_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_dt_pb_port, 8087),
    Bucket  = basho_bench_config:get(riakc_dt_pb_bucket, {<<"riak_dt">>,
                                                          <<"test">>}),
    BatchSize = basho_bench_config:get(riakc_dt_pb_sets_batchsize, 100),
    Preload = basho_bench_config:get(riakc_dt_preload_sets, false),
    PreloadNum = basho_bench_config:get(riakc_dt_preload_sets_num, 10),
    MaxValsForPreloadSet = basho_bench_config:get(
                             riakc_dt_max_vals_for_preload, 100),
    RunOneSet = basho_bench_config:get(riakc_dt_run_one_set, false),

    ValueGenTup = basho_bench_config:get(value_generator),
    StartBinSize = set_start_bin_size(ValueGenTup),

    %% Choose the target node using our ID as a modulus
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    lager:info("Ips: ~p Targets: ~p\n", [Ips, Targets]),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort, get_connect_options()) of
        {ok, Pid} ->
            PreloadedSets = case Preload of
                                true ->
                                    preload_sets(PreloadNum, Pid, Bucket,
                                                 StartBinSize);
                                false ->
                                    []
                            end,
            if RunOneSet ->
               run_one_set(Pid, Bucket, StartBinSize);
               true -> ok
            end,
            {ok, #state{ pid = Pid,
                         bucket = Bucket,
                         last_key=undefined,
                         batch_size = BatchSize,
                         preload = Preload,
                         preloaded_sets = PreloadedSets,
                         preloaded_sets_num = PreloadNum,
                         last_preload_nth = 0,
                         max_vals_for_preload = MaxValsForPreloadSet,
                         run_one_set = RunOneSet
                       }};
        {error, Reason} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason])
    end.

run({set, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                             run_one_set=true}=State) ->
    case riakc_pb_socket:fetch_type(Pid, Bucket, ?DEFAULT_SET_KEY) of
        {ok, Set0} ->
            Val = ValueGen(),
            Set1 = riakc_set:add_element(Val, Set0),
            update_type(Pid, Bucket, ?DEFAULT_SET_KEY, Set1, State);
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
                    update_type(Pid, Bucket, SetKey, Set1, State,
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
    update_type(Pid, Bucket, SetKey, Set1, State);

%% Note: Make sure to not use sequential for keys when run
run({set, batch_insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                  last_key=LastKey,
                                                  batch_size=BatchSize}=State) ->
    {SetKey, Members} = gen_set_batch(KeyGen, ValueGen, LastKey, BatchSize),
    Set0 = riakc_set:new(),
    SetLast = lists:foldl(fun(Elem, Sets) ->
                              Sets ++ [riakc_set:add_element(Elem, Set0)]
                         end, [], Members),
    State2 = State#state{last_key=SetKey},
    update_type(Pid, Bucket, SetKey, lists:last(SetLast), State2);

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
    fetch_action(Pid, Bucket, ?DEFAULT_SET_KEY, ValueGen, State, remove);
run({set, remove}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, preload=true, preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum}=State) ->
    SetKey = lists:nth(random:uniform(PreloadedSetsNum), PreloadedSets),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, remove);
run({set, remove}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, remove);

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
    fetch_action(Pid, Bucket, SetKey, ValueGen, State, is_element).

%%%===================================================================
%%% Private
%%%===================================================================

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).

%% @private generate a tuple w/ a set-key and a batch of members from the valgen
%%          NOTE: to be used w/ non-sequential `key_generator` keygen, otherwise
%%          the reset for the exausted valgen will reset the key_generator
gen_set_batch(KeyGen, ValueGen, LastKey, BatchSize) ->
    case {LastKey, gen_members(BatchSize, ValueGen)} of
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
    end.

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

%% @private preload and update riak with an N-number of set keys,
%% named in range <<"1..Nset">>.
preload_sets(N, Pid, Bucket, StartBinSize) ->
    SetKeys = [begin X1 = integer_to_binary(X),
                  Y = <<"set">>,
                  <<X1/binary,Y/binary>> end || X <- lists:seq(1, N)],
    [begin
         case riakc_pb_socket:fetch_type(Pid, Bucket, SetKey) of
             {error, _} ->
                 Set0 = riakc_set:new(),
                 Set1 = riakc_set:add_element(crypto:rand_bytes(StartBinSize),
                                              Set0),
            case update_type(Pid, Bucket, SetKey, Set1) of
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
     end || SetKey <- SetKeys].

%% @private check by fetching single-set run key before creating a new one.
run_one_set(Pid, Bucket, StartBinSize) ->
    case riakc_pb_socket:fetch_type(Pid, Bucket, ?DEFAULT_SET_KEY) of
        {error, _} ->
            Set0 = riakc_set:new(),

            %% can't be unmodified
            Set1 = riakc_set:add_element(crypto:rand_bytes(StartBinSize), Set0),
            case update_type(Pid, Bucket, ?DEFAULT_SET_KEY, Set1) of
                {ok, _} ->
                    ?INFO("~p created", [?DEFAULT_SET_KEY]);
                {error, Reason} ->
                    ?ERROR("~p not created b/c ~p", [?DEFAULT_SET_KEY,
                                                     Reason])
            end;
        _ ->
            ?INFO("~p Already Loaded", [?DEFAULT_SET_KEY])
    end.

%% @private starter binary size for set-preload.
set_start_bin_size(VGTup) ->
    StartBinSize =
        case VGTup of
            {var_bin_set, Lambda, _, poisson} ->
                basho_bench_valgen:poisson(Lambda);
            {var_bin_set, Lambda, LambdaThresh, _, poisson} ->
                basho_bench_valgen:poisson(Lambda, LambdaThresh);
            {var_bin_set, Min, Mean, _, exponential} ->
                basho_bench_valgen:exponential(Min, Mean);
            {var_bin_set, Min, Max, _} ->
                crypto:rand_uniform(Min, Max+1);
            {uniform_bin, Min, Max} ->
                crypto:rand_uniform(Min, Max+1);
            {exponential_bin, Min, Mean} ->
                basho_bench_valgen:exponential(Min, Mean);
            {fixed_bin_set, Size, _} ->
                Size;
            {_, Min, Max} when is_integer(Min), is_integer(Max) ->
                crypto:rand_uniform(Min, Max+1);
            {_, Size} when is_integer(Size) ->
                Size;
            _ -> 4
        end,
    ?DEBUG("StartBinSize: ~p\n", [StartBinSize]),
    StartBinSize.

%% @private fetch helper for read, remove, is_element runs
fetch_action(Pid, Bucket, Key, ValueGen, State, Action) ->
    case riakc_pb_socket:fetch_type(Pid, Bucket, Key) of
        {ok, Set0} ->
            case Action of
                remove ->
                    Val = ValueGen(),
                    Set1 = riakc_set:del_element(Val, Set0),
                    update_type(Pid, Bucket, Key, Set1, State);
                is_element ->
                    Val = ValueGen(),
                    riakc_set:is_element(Val, Set0),
                    {ok, State};
                _ ->
                    {ok, State}
            end;
        {error, {notfound, _}} ->
            {ok, State};
        {error, Reason} ->
            {error, {Reason, Key}, State}
    end.

%% @private riac_pb_socket:udpate_type wrapper
update_type(Pid, Bucket, Key, SetToOp) ->
    update_type(Pid, Bucket, Key, SetToOp, no_state).
update_type(Pid, Bucket, Key, SetToOp, State) ->
    update_type(Pid, Bucket, Key, SetToOp, State, State).
update_type(Pid, Bucket, Key, SetToOp, StateOnError, StateOnUpdate) ->
    case riakc_pb_socket:update_type(Pid, Bucket, Key, riakc_set:to_op(SetToOp)) of
        ok ->
            {ok, StateOnUpdate};
        {error, Reason} ->
            {error, {Reason, Key, update_type}, StateOnError}
    end.

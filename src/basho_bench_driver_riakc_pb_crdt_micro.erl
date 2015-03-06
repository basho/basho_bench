% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_pb: Driver for riak protocol buffers client
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
-module(basho_bench_driver_riakc_pb_crdt_micro).

-export([new/1,
         run/4
         ]).

-export([generate_repeated_bytes/2]).

-include("basho_bench.hrl").

%% This driver is intended to be used without concurrency, as there is no
%% synchronization between clients.

-record(state, { id,
                 pid,
                 bucket_map,
                 bucket_set,
                 bucket_register,
                 bucket_counter,
                 r,
                 pr,
                 w,
                 dw,
                 pw,
                 rw,
                 content_type,
                 search_queries,
                 query_step_interval,
                 start_time,
                 keylist_length,
                 preloaded_keys,
                 timeout_general,
                 timeout_read,
                 timeout_write,
                 timeout_listkeys,
                 timeout_mapreduce,
                 map_depth_gen,
                 txn_size_gen,
                 update_rate_gen,
                 fields_name_gen,
                 reg_gen,
                 ops_freq,
                 binsize_gen,
                 object,
                 delta_object
               }).

-define(TIMEOUT_GENERAL, 62*1000).              % Riak PB default + 2 sec

-define(ERLANG_MR,
        [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
         {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}]).
-define(JS_MR,
        [{map, {jsfun, <<"Riak.mapValuesJson">>}, none, false},
         {reduce, {jsfun, <<"Riak.reduceSum">>}, none, true}]).

-define(OPTIONS, [
                  %%{r,2},
                  {notfound_ok, true}, {timeout, 5000}
                 ]).

-define(COUNTER, riak_dt_pncounter).
-define(SET, riak_dt_orswot).
-define(DELTA_SET, riak_dt_delta_orswot).
-define(MAP, riak_dt_map).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    Replies = basho_bench_config:get(riakc_pb_replies, quorum),
    R = basho_bench_config:get(riakc_pb_r, Replies),
    W = basho_bench_config:get(riakc_pb_w, Replies),
    DW = basho_bench_config:get(riakc_pb_dw, Replies),
    RW = basho_bench_config:get(riakc_pb_rw, Replies),
    PW = basho_bench_config:get(riakc_pb_pw, Replies),
    PR = basho_bench_config:get(riakc_pb_pr, Replies),
    SearchQs = basho_bench_config:get(riakc_pb_search_queries, []),
    SearchQStepIval = basho_bench_config:get(query_step_interval, 60),
    KeylistLength = basho_bench_config:get(riakc_pb_keylist_length, 1000),
    PreloadedKeys = basho_bench_config:get(
                      riakc_pb_preloaded_keys, undefined),
    CT = basho_bench_config:get(riakc_pb_content_type, "application/octet-stream"),

    {MinDepth, MaxDepth} = basho_bench_config:get(map_depth, {1,1}),
    {MinTxnSize, MaxTxnSize} = basho_bench_config:get(transaction_size, {1,1}),
    {MinSizeBytes, MaxSizeBytes} = basho_bench_config:get(reg_size_in_bytes, {4,4}),
    FieldsNameDomain = basho_bench_config:get(fields_domain, 100),

    MapDepthGen = fun() -> MinDepth - 1 + random:uniform(1 + MaxDepth -  MinDepth) end,
    TxnSizeGen = fun() -> MinTxnSize - 1 + random:uniform(1 + MaxTxnSize -  MinTxnSize) end,
    RegGen = fun() ->
                     NBytes = MinSizeBytes + random:uniform(1 + MaxSizeBytes -  MinSizeBytes),
                     base64:encode(crypto:strong_rand_bytes(NBytes))
             end,

    BinaryWithSizeGen = fun() ->
                                Field = random:uniform(FieldsNameDomain),
                                FieldBin = integer_to_binary(Field),
                                NBytes = MinSizeBytes - 1 + random:uniform(1 + MaxSizeBytes -  MinSizeBytes),
                                generate_repeated_bytes(FieldBin, NBytes)
                        end,

    FieldsNameGen = fun() ->
                           integer_to_list(random:uniform(FieldsNameDomain))
                    end,
    OpsFreqPerDt = basho_bench_config:get(ops_frequency, []),
    OpsFreqComputed  = process_ops_freq(OpsFreqPerDt),

    BucketMap = basho_bench_config:get(riakc_pb_bucket_map, <<"test">>),
    BucketSet = basho_bench_config:get(riakc_pb_bucket_set, <<"test">>),
    BucketCounter = basho_bench_config:get(riakc_pb_bucket_counter, <<"test">>),
    BucketRegister = basho_bench_config:get(riakc_pb_bucket_register, <<"test">>),

    %% Choose the target node using our ID as a modulus
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort, get_connect_options()) of
        {ok, Pid} ->
            {ok, #state { id = Id,
                          pid = Pid,
                          bucket_map = BucketMap,
                          bucket_set = BucketSet,
                          bucket_register = BucketRegister,
                          bucket_counter = BucketCounter,
                          r = R,
                          pr = PR,
                          w = W,
                          dw = DW,
                          rw = RW,
                          pw = PW,
                          content_type = CT,
                          search_queries = SearchQs,
                          query_step_interval = SearchQStepIval,
                          start_time = erlang:now(),
                          keylist_length = KeylistLength,
                          preloaded_keys = PreloadedKeys,
                          timeout_general = get_timeout_general(),
                          timeout_read = get_timeout(pb_timeout_read),
                          timeout_write = get_timeout(pb_timeout_write),
                          timeout_listkeys = get_timeout(pb_timeout_listkeys),
                          timeout_mapreduce = get_timeout(pb_timeout_mapreduce),
                          fields_name_gen = FieldsNameGen,
                          txn_size_gen = TxnSizeGen,
                          map_depth_gen = MapDepthGen,
                          reg_gen = RegGen,
                          binsize_gen = BinaryWithSizeGen,
                          ops_freq = OpsFreqComputed
                        }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

run({{delta, set}, micro_op}, _KeyGen, ValueGen, State) ->
    Set = case State#state.delta_object of
              undefined -> ?DELTA_SET:new();
              Object -> Object
          end,

    %Result = execute_micro({delta, set}, Set, ValueGen, State),
    Result = execute_micro({delta, set}, Set, ValueGen, State),
    case Result of
        {ok, UpdtSet} ->
            {ok, State#state{delta_object=UpdtSet}};
        {error, Reason} ->
            ?INFO("Error on Set write ~p", [Reason]),
            {error, Reason, State}
    end;

run({{crdt, set}, micro_op}, _KeyGen, ValueGen, State) ->
    Set = case State#state.object of
              undefined -> ?SET:new();
              Object -> Object
          end,

    Result = execute_micro({crdt, set}, Set, ValueGen, State),

    case Result of
        {ok, UpdtSet} ->
            {ok, State#state{object=UpdtSet}};
        {error, Reason} ->
            ?INFO("Error on Set write ~p", [Reason]),
            {error, Reason, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

execute_micro({crdt, set}, Set, _ValueGen, State) ->
    TxnSize = (State#state.txn_size_gen)(),
    NextOp = next_op(State#state.ops_freq, set),

    case gen_micro_op(crdt, NextOp, State#state.binsize_gen, Set, TxnSize) of
        {_, undefined} -> {ok, Set};
        Op ->
            ?INFO("OPS ~p User ~p Context ~p~n",[Op, State#state.id, ?SET:precondition_context(Set)]),
            ?SET:update(Op, State#state.id, Set, ?SET:precondition_context(Set))
    end;

execute_micro({delta, set}, Set, _ValueGen, State) ->
    TxnSize = (State#state.txn_size_gen)(),
    NextOp = next_op(State#state.ops_freq, set),

    case gen_micro_op(delta, NextOp, State#state.binsize_gen, Set, TxnSize) of
        {_, undefined} -> {ok, Set};
        Op ->
            ?INFO("OPS ~p User ~p Context ~p~n",[Op, State#state.id, ?DELTA_SET:precondition_context(Set)]),
            {ok, Updt}= ?DELTA_SET:delta_update(Op, State#state.id, Set, ?DELTA_SET:precondition_context(Set)),
            {ok, ?DELTA_SET:merge(Set, Updt)}
    end.

gen_micro_op(_Type, {set, add}, ValueGen, _Set, 1) ->
    {add, ValueGen()};

gen_micro_op(crdt, {set, remove}, _, Set, 1) ->
    ElementSet = ?SET:value(Set),
    Op = case length(ElementSet) of
             0 -> undefined;
             _ -> lists:nth(random:uniform(length(ElementSet)),ElementSet)
         end,
    {remove, Op};

gen_micro_op(delta, {set, remove}, _, Set, 1) ->
    ElementSet = ?DELTA_SET:value(Set),
    Op = case length(ElementSet) of
             0 -> undefined;
             _ -> lists:nth(random:uniform(length(ElementSet)),ElementSet)
         end,
    {remove, Op};

gen_micro_op(_Type, {set, add}, ValueGen, _Set, TxnSize) ->
    SetOps = lists:foldl(fun(_, OpsAcc) ->
                                 [ValueGen() | OpsAcc]
                         end, [], lists:seq(1,TxnSize)),
    {add_all, SetOps};

gen_micro_op(crdt, {set, remove}, _, Set, TxnSize) ->
    ElementSet = ?SET:value(Set),
    SetOps = lists:foldl(fun(_, OpsAcc) ->
                                 case length(ElementSet) of
                                     0 -> OpsAcc;
                                     _ -> DelElem = lists:nth(random:uniform(length(ElementSet)),ElementSet),
                                          [DelElem | OpsAcc]
                                 end
                         end, [], lists:seq(1,TxnSize)),
    {remove_all, SetOps};

gen_micro_op(delta, {set, remove}, _, Set, TxnSize) ->
    ElementSet = ?DELTA_SET:value(Set),
    SetOps = lists:foldl(fun(_, OpsAcc) ->
                                 case length(ElementSet) of
                                     0 -> OpsAcc;
                                     _ -> DelElem = lists:nth(random:uniform(length(ElementSet)),ElementSet),
                                          [DelElem | OpsAcc]
                                 end
                         end, [], lists:seq(1,TxnSize)),
    {remove_all, SetOps}.

next_op({DtProbRange, _}, DtName) ->
    {_, {OpsProbRange,SumOpsRange},_} = lists:keyfind(DtName, 1, DtProbRange),
    R1 = random:uniform(SumOpsRange),
    [{Op, _} | _] = lists:dropwhile(fun({_, SumOp}) -> SumOp < R1 end, OpsProbRange),
    {DtName, Op}.

%next_op({DtProbRange, SumDtRange}=OpsFreq) ->
%    R0 = random:uniform(SumDtRange),
%    [{DtName, _, _} | _] = lists:dropwhile(
%                             fun({_, _, SumDt}) -> SumDt < R0
%                             end, DtProbRange),
%    next_op(OpsFreq, DtName).

process_ops_freq(OpsFreqPerDt) ->
    lists:mapfoldl(
                         fun({Dt, {Ops, DtFreq}}, DtAcc) ->
                                 {OpsUpdt, SumOp} = lists:mapfoldl(
                                                      fun({Op, OpFreq}, OpFreqAcc) ->
                                                              SumOp = OpFreq+OpFreqAcc,
                                                              {{Op, SumOp}, SumOp}
                                                      end, 0, Ops),
                                 SumDt = DtFreq + DtAcc,
                                 {{Dt, {OpsUpdt, SumOp}, SumDt}, SumDt}
                         end, 0, OpsFreqPerDt).

%%Possibly expensive computation, but does not require memory and
%%has equal cost for both solutions.
generate_repeated_bytes(Bytes, TotalLength) ->
    BytesLength = size(Bytes),
    NConcat = case TotalLength >= BytesLength of
                  true ->
                      R = TotalLength / BytesLength,
                      case trunc(R) < R of
                          true -> R+1;
                          _ -> R
                      end;
                  _ -> 1
              end,
    lists:foldl(fun(_,Acc) -> <<Bytes/binary, Acc/binary>> end,
                Bytes, lists:seq(2, trunc(NConcat))).

get_timeout_general() ->
    basho_bench_config:get(pb_timeout_general, ?TIMEOUT_GENERAL).

get_timeout(Name) when Name == pb_timeout_read;
                       Name == pb_timeout_write;
                       Name == pb_timeout_listkeys;
                       Name == pb_timeout_mapreduce ->
    basho_bench_config:get(Name, get_timeout_general()).

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).


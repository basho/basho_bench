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
-module(basho_bench_driver_riakc_pb_crdt).

-export([new/1,
         run/4
         ]).

-include("basho_bench.hrl").

-record(state, { pid,
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
                 ops_freq
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

    MapDepthGen = fun() -> random:uniform(1 + MaxDepth -  MinDepth) end,
    TxnSizeGen = fun() -> random:uniform(1 + MaxTxnSize -  MinTxnSize) end,
    RegGen = fun() ->
                     NBytes = random:uniform(1 + MaxSizeBytes -  MinSizeBytes),
                     base64:encode(crypto:strong_rand_bytes(NBytes))
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
            {ok, #state { pid = Pid,
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
                          ops_freq = OpsFreqComputed
                        }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

run({map, update, datatype}, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Result = riakc_pb_socket:fetch_type(State#state.pid,
                                        State#state.bucket_map,
                                        Key,
                                        ?OPTIONS),
    Result2 = case Result of
                  {ok, M0} ->
                      ?INFO("Map value ~p",[riakc_map:value(M0)]),
                      execute(map, Key, M0, ValueGen, State);
                  {error, {notfound, _}} ->
                      M0 = riakc_map:new(),
                      execute(map, Key, M0, ValueGen, State);
                  {error, Reason} ->
                      ?INFO("Error on Map read ~p",[Reason]),
                      {error, Reason, State}
              end,

    case Result2 of
        ok ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason2} ->
            ?INFO("Error on Map write ~p", [Reason2]),
            {error, Reason2, State}
    end;

run({set, op}, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Result = riakc_pb_socket:fetch_type(State#state.pid,
                                        State#state.bucket_set,
                                        Key,
                                        ?OPTIONS),

    Result2 = case Result of
                  {ok, S0} ->
                      ?INFO("Set value ~p~n",[riakc_set:value(S0)]),
                      execute(set, Key, S0, ValueGen, State);
                  {error, {notfound, _}} ->
                      S0 = riakc_set:new(),
                      execute(set, Key, S0, ValueGen, State);
                  {error, Reason} ->
                      ?INFO("Error on Set read ~p",[Reason]),
                      {error, Reason, State}
              end,

    case Result2 of
        ok ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason2} ->
            ?INFO("Error on Set write ~p", [Reason2]),
            {error, Reason2, State}
    end;

run({counter, op}, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Result = riakc_pb_socket:fetch_type(State#state.pid,
                                        State#state.bucket_counter,
                                        Key,
                                        ?OPTIONS),

    Result2 = case Result of
                  {ok, C0} ->
                      ?INFO("Counter value ~p",[riakc_counter:value(C0)]),
                      execute(counter, Key, C0, ValueGen, State);
                  {error, {notfound, _}} ->
                      C0 = riakc_counter:new(),
                      execute(counter, Key, C0, ValueGen, State);
                  {error, Reason} ->
                      ?INFO("Error on Counter read ~p",[Reason]),
                      {error, Reason, State}
              end,

    case Result2 of
        ok ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason2} ->
            ?INFO("Error on Counter write ~p", [Reason2]),
            {error, Reason2, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

execute(map, Key, Map0, ValueGen, State) ->
    TxnSize = (State#state.txn_size_gen)(),
    Depth = (State#state.map_depth_gen)(),
    Fields = lists:foldl(fun(_, FieldsAcc) ->
                                 ["Field_" ++ (State#state.fields_name_gen)() | FieldsAcc]
                         end, [], lists:seq(1,Depth)),

    Map = lists:foldl(fun(_, Mapi) ->
                              NextOp = next_op(State#state.ops_freq),
                              %%ups...
                              case NextOp of
                                  {register,_} ->
                                      execute_op(Fields, NextOp, State#state.reg_gen, Mapi);
                                  _ ->
                                      execute_op(Fields, NextOp, ValueGen, Mapi)
                              end
                      end, Map0, lists:seq(1,TxnSize)),

    riakc_pb_socket:update_type(State#state.pid,
                                State#state.bucket_map, Key, riakc_map:to_op(Map));

execute(set, Key, Set0, ValueGen, State) ->
    TxnSize = (State#state.txn_size_gen)(),
    Set = lists:foldl(fun(_, Seti) ->
                              NextOp = next_op(State#state.ops_freq, set),
                              execute_op(NextOp, ValueGen, Seti)
                      end, Set0, lists:seq(1,TxnSize)),
    riakc_pb_socket:update_type(State#state.pid,
                                State#state.bucket_set, Key, riakc_set:to_op(Set));

execute(counter, Key, Counter0, ValueGen, State) ->
    TxnSize = (State#state.txn_size_gen)(),
    Counter = lists:foldl(fun(_, Counteri) ->
                                  NextOp = next_op(State#state.ops_freq, counter),
                                  execute_op(NextOp, ValueGen, Counteri)
                          end, Counter0, lists:seq(1, TxnSize)),
    riakc_pb_socket:update_type(State#state.pid,
                                State#state.bucket_counter, Key, riakc_counter:to_op(Counter));

execute(register, Key, Register0, _ValueGen, State) ->
    TxnSize = (State#state.txn_size_gen)(),
    Register = lists:foldl(fun(_, Registeri) ->
                                   NextOp = next_op(State#state.ops_freq, register),
                                   execute_op(NextOp, State#state.reg_gen, Registeri)
                           end, Register0, lists:seq(1, TxnSize)),
    riakc_pb_socket:update_type(State#state.pid,
                                State#state.bucket_register, Key, riakc_register:to_op(Register)).

execute_op({counter, increment}, _, Counter) ->
    riakc_counter:increment(1, Counter);

execute_op({counter, decrement}, _, Counter) ->
    riakc_counter:decrement(1, Counter);

execute_op({set, put}, ValueGen, Set) ->
    riakc_set:add_element(integer_to_binary(ValueGen()), Set);

execute_op({set, remove}, _, Set) ->
    ElementSet = riakc_set:dirty_value(Set),
    case length(ElementSet) of
        0 -> Set;
        _ -> DelElem = lists:nth(random:uniform(length(ElementSet)),ElementSet),
             riakc_set:del_element(DelElem, Set)
    end;

execute_op({register, update}, ValueGen, Register) ->
    riakc_register:set(ValueGen(), Register);

execute_op({flag, enable}, _ValueGen, Flag) ->
    riakc_flag:enable(Flag);

execute_op({flag, disable}, _ValueGen, Flag) ->
    riakc_flag:disable(Flag).

next_op({DtProbRange, _}, DtName) ->
    {_, {OpsProbRange,SumOpsRange},_} = lists:keyfind(DtName, 1, DtProbRange),
    R1 = random:uniform(SumOpsRange),
    [{Op, _} | _] = lists:dropwhile(fun({_, SumOp}) -> SumOp < R1 end, OpsProbRange),
    {DtName, Op}.

next_op({DtProbRange, SumDtRange}=OpsFreq) ->
    R0 = random:uniform(SumDtRange),
    [{DtName, _, _} | _] = lists:dropwhile(
                             fun({_, _, SumDt}) -> SumDt < R0
                             end, DtProbRange),
    next_op(OpsFreq, DtName).

execute_op([FieldName], {Type, _Op} = TypeOp, ValueGen, Map) ->
    riakc_map:update(
      {FieldName, Type},
      fun(T) ->
           execute_op(TypeOp, ValueGen, T)
      end, Map);

execute_op([Head | Tail], Op, ValueGen, Map) ->
    riakc_map:update(
      {Head, map},
      fun(M) ->
              execute_op(Tail, Op, ValueGen, M)
      end, Map).

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


get_timeout_general() ->
    basho_bench_config:get(pb_timeout_general, ?TIMEOUT_GENERAL).

get_timeout(Name) when Name == pb_timeout_read;
                       Name == pb_timeout_write;
                       Name == pb_timeout_listkeys;
                       Name == pb_timeout_mapreduce ->
    basho_bench_config:get(Name, get_timeout_general()).

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).


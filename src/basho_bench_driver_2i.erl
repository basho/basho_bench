%% -------------------------------------------------------------------
%%
%% basho_bench_driver_2i_pb: Driver for Secondary Indices (via PB)
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
-module(basho_bench_driver_2i).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 r,
                 w,
                 dw,
                 rw,
                 num_integer_indexes,
                 num_binary_indexes,
                 binary_index_size
               }).


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
    Replies = basho_bench_config:get(riakc_pb_replies, 2),
    R = basho_bench_config:get(riakc_pb_r, Replies),
    W = basho_bench_config:get(riakc_pb_w, Replies),
    DW = basho_bench_config:get(riakc_pb_dw, Replies),
    RW = basho_bench_config:get(riakc_pb_rw, Replies),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),

    %% How many indexes should we make?
    NumIntegerIndexes = basho_bench_config:get(num_integer_indexes, 3),
    NumBinaryIndexes = basho_bench_config:get(num_binary_indexes, 3),
    BinaryIndexSize = basho_bench_config:get(binary_index_size, 20),

    %% Choose the node using our ID as a modulus
    TargetIp = lists:nth((Id rem length(Ips)+1), Ips),
    ?INFO("Using target ip ~p for worker ~p\n", [TargetIp, Id]),

    case riakc_pb_socket:start_link(TargetIp, Port) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          r = R,
                          w = W,
                          dw = DW,
                          rw = RW,
                          num_integer_indexes = NumIntegerIndexes,
                          num_binary_indexes = NumBinaryIndexes,
                          binary_index_size = BinaryIndexSize
                         }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p port ~p: ~p\n",
                      [TargetIp, Port, Reason2])
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}]) of
        {ok, Obj} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    %% Generate key, value, and metadata...
    Key = KeyGen(),
    Value = ValueGen(),
    Indexes =
        generate_integer_indexes(Key, State#state.num_integer_indexes) ++
        generate_binary_indexes(Key, State#state.num_binary_indexes, State#state.binary_index_size),
    MetaData = dict:from_list([{<<"index">>, Indexes}]),

    %% Create the object...
    Robj0 = riakc_obj:new(State#state.bucket, Key),
    Robj1 = riakc_obj:update_value(Robj0, Value),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),

    %% Write the object...
    case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                     {dw, State#state.dw}]) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(int_eq_query, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    [{Field,Term}|_] =
        generate_integer_indexes(Key,
                                 State#state.num_integer_indexes),
    case riakc_pb_socket:get_index(State#state.pid,
                                   State#state.bucket,
                                   Field,
                                   Term) of
        {ok, Results} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(int_range_query, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    [{Field, StartTerm},{_, EndTerm}|_] =
        generate_integer_indexes(Key,
                                 State#state.num_integer_indexes),
    case riakc_pb_socket:get_index(State#state.pid,
                                   State#state.bucket,
                                   Field,
                                   lists:min([StartTerm, EndTerm]),
                                   lists:max([StartTerm, EndTerm])) of
        {ok, Results} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(bin_eq_query, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    _Value = ValueGen(),
    [{Field, Term}|_] =
        generate_binary_indexes(Key,
                                State#state.num_binary_indexes,
                                State#state.binary_index_size),
    case riakc_pb_socket:get_index(State#state.pid,
                                   State#state.bucket,
                                   Field,
                                   Term) of
        {ok, Results} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(bin_range_query, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    _Value = ValueGen(),
    [{Field, StartTerm},{_, EndTerm}|_] =
        generate_binary_indexes(Key,
                                State#state.num_binary_indexes,
                                State#state.binary_index_size),
    case riakc_pb_socket:get_index(State#state.pid,
                                   State#state.bucket,
                                   Field,
                                   lists:min([StartTerm, EndTerm]),
                                   lists:max([StartTerm, EndTerm])) of
        {ok, Results} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(Other, _, _, _) ->
    throw({unknown_operation, Other}).

%% ====================================================================
%% Internal functions
%% ====================================================================

generate_integer_indexes(_, 0) ->
    [];
generate_integer_indexes(Seed, N) when is_binary(Seed) ->
    %% Pull a field value out of the binary. In this case, a 32-bit integer...
    <<V:32/integer, _/binary>> = Seed,

    %% Create the field name...
    K = list_to_binary("field" ++ integer_to_list(N) ++ "_int"),

    %% Loop.
    [{K,V}|generate_integer_indexes(erlang:md5(Seed), N - 1)];
generate_integer_indexes(Seed, _) when is_binary(Seed) ->
    throw({invalid_value, "Seed value must be a binary."}).


generate_binary_indexes(_, 0, _) ->
    [];
generate_binary_indexes(Seed, N, Size) when is_binary(Seed) ->
    %% Pull a field value out of the binary...
    V1 = generate_binary_index(Seed, Size),

    %% Create the field name and normalize the value...
    K = list_to_binary("field" ++ integer_to_list(N) ++ "_bin"),
    V2 = normalize_binary_index(V1),

    %% Loop.
    [{K,V2}|generate_binary_indexes(erlang:md5(Seed), N - 1, Size)];
generate_binary_indexes(Seed, _N, _Size) when is_binary(Seed) ->
    throw({invalid_value, "Seed value must be a binary."}).

generate_binary_index(Seed, Size) ->
    iolist_to_binary(generate_binary_index_1(Seed, Size)).
generate_binary_index_1(_Seed, 0) ->
    [];
generate_binary_index_1(Seed, Size) when Size >= 16 ->
    NewSeed = erlang:md5(Seed),
    [NewSeed|generate_binary_index_1(NewSeed, Size - 16)];
generate_binary_index_1(Seed, Size) ->
    NewSeed = erlang:md5(Seed),
    <<V:Size/binary, _/binary>> = NewSeed,
    [V].



normalize_binary_index(Value) ->
    normalize_binary_index(Value, <<>>).
normalize_binary_index(<<C, Rest/binary>>, Acc)
  when C >= $a andalso C =< $z;
       C >= $A andalso C =< $Z;
       C >= $0 andalso C =< $9 ->
    normalize_binary_index(Rest, <<Acc/binary, <<C>>/binary>>);
normalize_binary_index(<<_, Rest/binary>>, Acc) ->
    normalize_binary_index(Rest, <<Acc/binary, <<$_>>/binary>>);
normalize_binary_index(<<>>, Acc) ->
    Acc.

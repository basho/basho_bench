%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2012 Basho Techonologies
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
-module(basho_bench_driver_shortcut).

-export([new/1,
         run/4]).
-export([count_eleveldb_keys/1, calc_bkey_to_prefidxes/4]).

-include("basho_bench.hrl").

-record(state, { id :: integer(),
                 backend :: 'bitcask' | 'eleveldb' | 'hanoidb',
                 backend_flags :: list(),
                 data_dir :: string(),
                 n_val :: integer(),
                 ring :: term(),
                 bucket :: binary(),
                 store_riak_obj :: boolean(),
                 idxes_to_do :: list(integer()),
                 idx :: integer(),
                 handle :: term()}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    Backend = basho_bench_config:get(shortcut_backend),
    BackendFlags = basho_bench_config:get(shortcut_backend_flags),
    DataDir = basho_bench_config:get(shortcut_data_dir),
    os:cmd("mkdir -p " ++ DataDir),
    RingSize = basho_bench_config:get(shortcut_ring_creation_size),
    N = basho_bench_config:get(shortcut_n_val),
    Ring = riak_core_ring:fresh(RingSize, nonode),
    Bucket = basho_bench_config:get(shortcut_bucket),
    StoreObj = basho_bench_config:get(shortcut_store_riak_object, false),
    Idxes = [Idx || {Idx, _} <- riak_core_ring:all_owners(Ring)],
    Concurrent = basho_bench_config:get(concurrent),
    IdxParts = partition_work(Idxes, Concurrent),
    %% Id starts counting at 1
    MyIdxes = lists:nth(Id, IdxParts),

    {ok, rotate_idx(#state{id = Id,
                           backend = Backend,
                           backend_flags = BackendFlags,
                           data_dir = DataDir,
                           n_val = N,
                           ring = Ring,
                           bucket = Bucket,
                           store_riak_obj = StoreObj,
                           idxes_to_do = MyIdxes})}.

run(put, KeyGen, ValueGen, S) ->
    try
        Key = filter_key_gen(KeyGen, S),
        Value = ValueGen(),
        do_put(Key, Value, S)
    catch
        throw:{stop, empty_keygen} ->
            ?DEBUG("Empty keygen\n", []),
            NewS = rotate_idx(S),
            do_put(filter_key_gen(KeyGen, NewS), ValueGen(), NewS)
   end.

%% Private functions

partition_work(L, Num) ->
    partition_work2(L, lists:duplicate(Num, [])).

partition_work2([], Acc) ->
    [lists:reverse(L) || L <- Acc];
partition_work2([H|T], [First|Rest] = _Res) ->
    partition_work2(T, Rest ++ [[H|First]]).

filter_key_gen(KeyGen, #state{ring = Ring, n_val = N, bucket = Bucket,
                              idx = Idx} = S) ->
    Key = KeyGen(),
    %% case KeyGen() of
    %%     {sext_pair, SextBKey, PlainKey} ->
    %%         HashKey = PlainKey,
    %%         Key = SextBKey, PlainKey;
    %%     Plain ->
    %%         HashKey = Key = Plain
    %% end,
    PrefIdxes = calc_bkey_to_prefidxes(Bucket, Key, Ring, N),
    case lists:member(Idx, PrefIdxes) of
        true  ->
            Key;
        false ->
            filter_key_gen(KeyGen, S)
    end.

rotate_idx(#state{idxes_to_do = []} = S) ->
    %% Borrow a trick from the key generator: we are really, really done now.
    stop_idx(S),
    throw({stop, empty_keygen});
rotate_idx(#state{backend = Backend,
                  backend_flags = BackendFlags,
                  data_dir = DataDir,
                  idxes_to_do = [Idx|Idxes]} = S0) ->
    S1 = stop_idx(S0),
    basho_bench_keygen:reset_sequential_int_state(),
    Handle = start_idx(Backend, BackendFlags, DataDir, Idx),
    S1#state{idxes_to_do = Idxes,
             idx = Idx,
             handle = Handle}.

stop_idx(#state{backend = Backend, handle = Handle} = S) ->
    try
        stop_backend(Backend, Handle)
    catch
        X:Y ->
            ?ERROR("Stopping Id ~p's handle ~p -> ~p ~p: ~p\n",
                   [S#state.id, Handle, X, Y, erlang:get_stacktrace()])
    end,
    S#state{handle = undefined}.

start_idx(eleveldb, Flags0, DataDir, Idx) ->
    Flags = [{create_if_missing, true}|Flags0],
    {ok, Handle} = eleveldb:open(DataDir ++ "/" ++ integer_to_list(Idx), Flags),
    Handle;
start_idx(bitcask, Flags0, DataDir, Idx) ->
    Flags = [read_write|Flags0],
    bitcask:open(DataDir ++ "/" ++ integer_to_list(Idx), Flags);
start_idx(hanoidb, Flags, DataDir, Idx) ->
    {ok, Handle} = hanoidb:open(DataDir ++ "/" ++ integer_to_list(Idx), Flags),
    Handle.

do_put(Key0, Value0, #state{backend = eleveldb, handle = Handle,
                            bucket = Bucket} = S) ->
    {Key, Value} = make_riak_object_maybe(Bucket, Key0, Value0, S),
    %% TODO: add an option for put options?
    case eleveldb:put(Handle, Key, Value, []) of
        ok ->
            {ok, S};
        {error, Reason} ->
            {error, Reason, S}
    end;
do_put(Key0, Value0, #state{backend = bitcask, handle = Handle,
                            bucket = Bucket} = S) ->
    {Key, Value} = make_riak_object_maybe(Bucket, Key0, Value0, S),
    case bitcask:put(Handle, Key, Value) of
        ok ->
            {ok, S};
        {error, Reason} ->
            {error, Reason, S}
    end;
do_put(Key0, Value0, #state{backend = hanoidb, handle = Handle,
                            bucket = Bucket} = S) ->
    {Key, Value} = make_riak_object_maybe(Bucket, Key0, Value0, S),
    case hanoidb:put(Handle, Key, Value) of
        ok ->
            {ok, S};
        {error, Reason} ->
            {error, Reason, S}
    end.

stop_backend(_, undefined) ->
    ok;
stop_backend(eleveldb, _Handle) ->
    %% Key = <<66:2048>>,
    %% ok = eleveldb:put(Handle, Key, <<>>, []),
    %% ok = eleveldb:delete(Handle, Key, [{sync, true}]),
    ok;
stop_backend(bitcask, Handle) ->
    ok = bitcask:close(Handle);
stop_backend(hanoidb, Handle) ->
    ok = hanoidb:close(Handle).

count_eleveldb_keys(Dir) ->
    [{File, begin
                {ok, L1} = eleveldb:open(Dir ++ "/" ++ File, []),
                eleveldb:fold_keys(L1, fun(_, Acc) -> Acc + 1 end, 0, [])
            end} || File <- filelib:wildcard("*", Dir)].

make_riak_object_maybe(_Bucket, Key, Value, #state{store_riak_obj = false}) ->
    {Key, Value};
make_riak_object_maybe(Bucket, Key, Value, #state{store_riak_obj = true,
                                                  backend = eleveldb}) ->
    new_object(sext:encode({o, Bucket, Key}), Bucket, Key, Value);
make_riak_object_maybe(Bucket, Key, Value, #state{store_riak_obj = true,
                                                  backend = bitcask}) ->
    new_object(term_to_binary({Bucket, Key}), Bucket, Key, Value);
make_riak_object_maybe(Bucket, Key, Value, #state{store_riak_obj = true,
                                                  backend = hanoidb}) ->
    new_object(sext:encode({o, Bucket, Key}), Bucket, Key, Value).

new_object(EncodedKey, Bucket, Key, Value) ->
    %% MD stuff stolen from riak_kv_put_fsm.erl
    Now = erlang:now(),
    <<HashAsNum:128/integer>> = basho_bench:md5(term_to_binary({node(), Now})),
    VT = riak_core_util:integer_to_list(HashAsNum,62),
    NewMD = dict:store(<<"X-Riak-VTag">>, VT,
                       dict:store(<<"X-Riak-Last-Modified">>, Now, dict:new())),
    {EncodedKey,
     term_to_binary(
       riak_object:increment_vclock(riak_object:new(Bucket, Key, Value,
                                                    NewMD),
                                    <<42:32/big>>))}.

calc_bkey_to_prefidxes(Bucket, Key, RingSize, N) when is_integer(RingSize) ->
    calc_bkey_to_prefidxes(Bucket, Key, riak_core_ring:fresh(RingSize, nonode),
                           N);
calc_bkey_to_prefidxes(Bucket, Key, Ring, N) ->
    DocIdx = riak_core_util:chash_std_keyfun({Bucket, Key}),
    Preflist = lists:sublist(riak_core_ring:preflist(DocIdx, Ring), N),
    [I || {I, _} <- Preflist].

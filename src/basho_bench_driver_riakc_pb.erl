%% -------------------------------------------------------------------
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
-module(basho_bench_driver_riakc_pb).

-export([new/1,
         run/4,
         mapred_valgen/2,
         mapred_ordered_valgen/1]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 r,
                 pr,
                 w,
                 dw,
                 pw,
                 rw,
                 keylist_length,
                 preloaded_keys,
                 timeout_general,
                 timeout_read,
                 timeout_write,
                 timeout_listkeys,
                 timeout_mapreduce
               }).

-define(TIMEOUT_GENERAL, 62*1000).              % Riak PB default + 2 sec

-define(ERLANG_MR,
        [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
         {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}]).
-define(JS_MR,
        [{map, {jsfun, <<"Riak.mapValuesJson">>}, none, false},
         {reduce, {jsfun, <<"Riak.reduceSum">>}, none, true}]).

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
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),
    KeylistLength = basho_bench_config:get(riakc_pb_keylist_length, 1000),
    PreloadedKeys = basho_bench_config:get(
                      riakc_pb_preloaded_keys, undefined),
    warn_bucket_mr_correctness(PreloadedKeys),

    %% Choose the target node using our ID as a modulus
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort, get_connect_options()) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          r = R,
                          pr = PR,
                          w = W,
                          dw = DW,
                          rw = RW,
                          pw = PW,
                          keylist_length = KeylistLength,
                          preloaded_keys = PreloadedKeys,
                          timeout_general = get_timeout_general(),
                          timeout_read = get_timeout(pb_timeout_read),
                          timeout_write = get_timeout(pb_timeout_write),
                          timeout_listkeys = get_timeout(pb_timeout_listkeys),
                          timeout_mapreduce = get_timeout(pb_timeout_mapreduce)
                        }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

%% @doc For bucket-wide MapReduce, we can only check the result for
%% correctness if we know how many keys are stored.  This will print a
%% warning if that information is not available (it's expected as
%% `riakc_pb_preloaded_keys' in the config).
warn_bucket_mr_correctness(undefined) ->
    Operations = basho_bench_config:get(operations),
    BucketMR = [ Op || {Op, _} <- Operations,
                       Op == mr_bucket_js orelse
                           Op == mr_bucket_erlang ],
    case BucketMR of
        [] ->
            %% no need to warn - no bucket-wide MR
            ok;
        _ ->
            ?WARN("Bucket-wide MapReduce operations are specified,"
                  " but riakc_pb_preloaded_keys is not."
                  " Results will not be checked for correctness.~n",
                  [])
    end;
warn_bucket_mr_correctness(_) ->
    %% preload is specified, so no warning necessary
    ok.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}], State#state.timeout_read) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(get_existing, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}], State#state.timeout_read) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {error, {not_found, Key}, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
    Robj = riakc_obj:update_value(Robj0, ValueGen()),
    case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                     {dw, State#state.dw}], State#state.timeout_write) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}], State#state.timeout_read) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}], State#state.timeout_write) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj0 = riakc_obj:new(State#state.bucket, Key),
            Robj = riakc_obj:update_value(Robj0, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                             {dw, State#state.dw}], State#state.timeout_write) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}], State#state.timeout_read) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}], State#state.timeout_write) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            {error, {not_found, Key}, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:delete(State#state.pid, State#state.bucket, KeyGen(),
                                [{rw, State#state.rw}], State#state.timeout_write) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(listkeys, _KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:list_keys(State#state.pid, State#state.bucket, State#state.timeout_listkeys) of
        {ok, _Keys} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(mr_bucket_erlang, _KeyGen, _ValueGen, State) ->
    mapred(State, State#state.bucket, ?ERLANG_MR);
run(mr_bucket_js, _KeyGen, _ValueGen, State) ->
    mapred(State, State#state.bucket, ?JS_MR);
run(mr_keylist_erlang, KeyGen, _ValueGen, State) ->
    Keylist = make_keylist(State#state.bucket, KeyGen,
                           State#state.keylist_length),
    mapred(State, Keylist, ?ERLANG_MR);
run(mr_keylist_js, KeyGen, _ValueGen, State) ->
    Keylist = make_keylist(State#state.bucket, KeyGen,
                           State#state.keylist_length),
    mapred(State, Keylist, ?JS_MR);
run(counter_incr, KeyGen, ValueGen, State) ->
    Amt = ValueGen(),
    Key = KeyGen(),
    case riakc_pb_socket:counter_incr(State#state.pid, State#state.bucket, Key, Amt,
                                      [{w, State#state.w},
                                       {dw, State#state.dw},
                                       {pw, State#state.pw}]) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(counter_val, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:counter_val(State#state.pid, State#state.bucket, Key,
                                     [{r, State#state.r}, {pr, State#state.pr}]) of
        {ok, _N} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

mapred(State, Input, Query) ->
    case riakc_pb_socket:mapred(State#state.pid, Input, Query, State#state.timeout_mapreduce) of
        {ok, Result} ->
            case check_result(State, Input, Query, Result) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

check_result(#state{preloaded_keys=Preload},
             Input, ?ERLANG_MR, Result) when is_binary(Input) ->
    case Preload of
        undefined -> %% can't check if we don't know
            ok;
        _ ->
            case [{1, [Preload]}] of
                Result -> %% ERLANG_MR counts inputs,
                    ok;   %% should equal # preloaded keys
                Expected ->
                    {error, {Expected, Result}}
            end
    end;
check_result(_State, Input, ?ERLANG_MR, Result) ->
    case [{1, [length(Input)]}] of
        Result ->
            ok;
        Expected ->
            {error, {Expected, Result}}
    end;
check_result(#state{preloaded_keys=Preload},
             Input, ?JS_MR, Result) when is_binary(Input) ->
    case Preload of
        undefined -> %% can't check if we don't know
            ok;
        _ ->
            %% NOTE: this is Preload-1 instead of Preload+1, as
            %% expected, because keys are 0 to (Preload-1),
            %% not 1 to Preload
            case [{1, [(Preload*(Preload-1)) div 2]}] of
                Result -> %% JS_MR sums inputs,
                    ok;
                Expected ->
                    {error, {Expected, Result}}
            end
    end;
check_result(_State, Input, ?JS_MR, Result) ->
    Sum = lists:sum([ list_to_integer(binary_to_list(I))
                      || {_, I} <- Input ]),
    case [{1, [Sum]}] of
        Result ->
            ok;
        Expected ->
            {error, {Expected, Result}}
    end.

make_keylist(_Bucket, _KeyGen, 0) ->
    [];
make_keylist(Bucket, KeyGen, Count) ->
    [{Bucket, list_to_binary(KeyGen())}
     |make_keylist(Bucket, KeyGen, Count-1)].

mapred_valgen(_Id, MaxRand) ->
    fun() ->
            list_to_binary(integer_to_list(random:uniform(MaxRand)))
    end.

%% to be used along with sequential_int keygen to populate known
%% mapreduce set
mapred_ordered_valgen(Id) ->
    Save = list_to_atom("mapred_ordered_valgen"++integer_to_list(Id)),
    fun() ->
            Next = case get(Save) of
                       undefined -> 0;
                       Value     -> Value
                   end,
            put(Save, Next+1),
            list_to_binary(integer_to_list(Next))
    end.

get_timeout_general() ->
    basho_bench_config:get(pb_timeout_general, ?TIMEOUT_GENERAL).

get_timeout(Name) when Name == pb_timeout_read;
                       Name == pb_timeout_write;
                       Name == pb_timeout_listkeys;
                       Name == pb_timeout_mapreduce ->
    basho_bench_config:get(Name, get_timeout_general()).

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).

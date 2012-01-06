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
         mapred_valgen/2]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 r,
                 w,
                 dw,
                 rw,
                 keylist_length,
                 res_on_get,
                 res_after_put,
                 res_during_update,
                 max_res_attempts,
                 res_timeout}).

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
    Replies = basho_bench_config:get(riakc_pb_replies, 2),
    R = basho_bench_config:get(riakc_pb_r, Replies),
    W = basho_bench_config:get(riakc_pb_w, Replies),
    DW = basho_bench_config:get(riakc_pb_dw, Replies),
    RW = basho_bench_config:get(riakc_pb_rw, Replies),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),
    KeylistLength = basho_bench_config:get(riakc_pb_keylist_length, 1000),
    %% Enables sibling resolution
    ResolveOnGet = basho_bench_config:get(riakc_pb_res_on_get, false),
    ResolveAfterPut = basho_bench_config:get(riakc_pb_res_after_put, false),
    ResolveDuringUpdate = basho_bench_config:get(riakc_pb_res_during_update, false),
    MaxResolveAttempts = basho_bench_config:get(riakc_pb_max_res_attemps, 1),
    ResolveTimeout = basho_bench_config:get(riakc_pb_res_timeout, 0),

    %% Choose the target node using our ID as a modulus
    Targets = expand_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          r = R,
                          w = W,
                          dw = DW,
                          rw = RW,
                          keylist_length = KeylistLength,
                          res_on_get = ResolveOnGet,
                          res_after_put = ResolveAfterPut,
                          res_during_update = ResolveDuringUpdate,
                          max_res_attempts = MaxResolveAttempts,
                          res_timeout = ResolveTimeout
                        }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}]) of
        {ok, RObj} ->
            case State#state.res_on_get andalso riakc_obj:value_count(RObj) > 1 of
                true -> resolve_siblings(RObj, State);
                false -> {ok, State}
            end;
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(get_existing, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}]) of
        {ok, RObj} ->
            case State#state.res_on_get andalso riakc_obj:value_count(RObj) > 1 of
                true -> resolve_siblings(RObj, State);
                false -> {ok, State}
            end;
        {error, notfound} ->
            {error, {not_found, Key}, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
    Robj = riakc_obj:update_value(Robj0, ValueGen()),

    PutOptions = [{w, State#state.w},
                  {dw, State#state.dw}|
                  case State#state.res_after_put of
                      true -> [return_body];
                      false -> []
                  end],

    case riakc_pb_socket:put(State#state.pid, Robj, PutOptions) of
        ok ->
            {ok, State};
        {ok, RObj} ->
            case riakc_obj:value_count(RObj) > 1 of
                true -> resolve_siblings(RObj, State);
                false -> {ok, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}]) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            Robj3 = case State#state.res_during_update 
                     andalso riakc_obj:value_count(Robj2) > 1 of
                true -> riakc_obj:select_sibling(1, Robj2);
                false -> Robj2
            end,

            PutOptions = [{w, State#state.w},
                          {dw, State#state.dw}|
                          case State#state.res_after_put of
                              true -> [return_body];
                              false -> []
                          end],

            case riakc_pb_socket:put(State#state.pid, Robj3, PutOptions) of
                ok ->
                    {ok, State};
                {ok, RObj} ->
                    case riakc_obj:value_count(RObj) > 1 of
                        true -> resolve_siblings(RObj, State);
                        false -> {ok, State}
                    end;
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
            Robj = riakc_obj:update_value(Robj0, ValueGen()),

            PutOptions = [{w, State#state.w},
                          {dw, State#state.dw}|
                          case State#state.res_after_put of
                              true -> [return_body];
                              false -> []
                          end],

            case riakc_pb_socket:put(State#state.pid, Robj, PutOptions) of
                ok ->
                    {ok, State};
                {ok, RObj} ->
                    case riakc_obj:value_count(RObj) > 1 of
                        true -> resolve_siblings(RObj, State);
                        false -> {ok, State}
                    end;
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}]) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            Robj3 = case State#state.res_during_update
                     andalso riakc_obj:value_count(Robj2) > 1 of
                true -> riakc_obj:select_sibling(1, Robj2);
                false -> Robj2
            end,

            PutOptions = [{w, State#state.w},
                          {dw, State#state.dw}|
                          case State#state.res_after_put of
                              true -> [return_body];
                              false -> []
                          end],

            case riakc_pb_socket:put(State#state.pid, Robj3, PutOptions) of
                ok ->
                    {ok, State};
                {ok, RObj} ->
                    case riakc_obj:value_count(RObj) > 1 of
                        true -> resolve_siblings(RObj, State);
                        false -> {ok, State}
                    end;
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            {error, {not_found, Key}, State}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:delete(State#state.pid, State#state.bucket, KeyGen(),
                                [{rw, State#state.rw}]) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(listkeys, _KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:list_keys(State#state.pid, State#state.bucket) of
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
    mapred(State, Keylist, ?JS_MR).

%% ====================================================================
%% Internal functions
%% ====================================================================

expand_ips(Ips, Port) ->
    lists:foldl(fun({Ip,Ports}, Acc) when is_list(Ports) ->
                        Acc ++ lists:map(fun(P) -> {Ip, P} end, Ports);
                   (T={_,_}, Acc) ->
                        [T|Acc];
                   (Ip, Acc) ->
                        [{Ip,Port}|Acc]
                end, [], Ips).

mapred(State, Input, Query) ->
    case riakc_pb_socket:mapred(State#state.pid, Input, Query) of
        {ok, _Result} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
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

resolve_siblings(RObj, State) ->
    resolve_siblings(RObj, State#state.max_res_attempts, State).

resolve_siblings(_, 0, State) -> {error, max_res_attempts, State};
resolve_siblings(RObj, AttemptsLeft, State) ->
    timer:sleep(State#state.res_timeout),

    ResolvedRObj = riakc_robj:select_sibling(1, RObj),
    case riakc_pb_socket:put(State#state.pid, ResolvedRObj, [{w, State#state.w},
                                                             {dw, State#state.dw},
                                                             return_body]) of
        {ok, ReturnedRObj} ->
            case riakc_obj:value_count(ReturnedRObj) > 1 of
                true -> resolve_siblings(ReturnedRObj, AttemptsLeft-1, State);
                false -> {ok, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

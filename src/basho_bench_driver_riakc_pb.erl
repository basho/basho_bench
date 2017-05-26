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
    SearchQs = basho_bench_config:get(riakc_pb_search_queries, []),
    SearchQStepIval = basho_bench_config:get(query_step_interval, 60),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),
    KeylistLength = basho_bench_config:get(riakc_pb_keylist_length, 1000),
    PreloadedKeys = basho_bench_config:get(
                      riakc_pb_preloaded_keys, undefined),
    CT = basho_bench_config:get(riakc_pb_content_type, "application/octet-stream"),
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

%% Write information about the team.
run({team, write}, KeyGen, _ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Map0 = riakc_map:new(),
    Map = riakc_map:update(
               {<<"name">>, register},
               fun(R) ->
                   riakc_register:set(
                       list_to_binary("Team " ++ Key), R)
               end, Map0),
    Result = riakc_pb_socket:update_type(State#state.pid,
             State#state.bucket, Key, riakc_map:to_op(Map)),
    case Result of
        ok ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            lager:info("Team write failed, error: ~p", [Reason]),
            {error, Reason, State}
    end;

%% Read information about the team.
run({team, read}, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Options = [{r,2}, {notfound_ok, true}, {timeout, 5000}],
    Result = riakc_pb_socket:fetch_type(State#state.pid,
                                        State#state.bucket,
                                        Key,
                                        Options),
    case Result of
        {ok, _} ->
            {ok, State};
        {error, {notfound, _}} ->
            lager:info("Team does not exist yet."),
            run({team, write}, KeyGen, ValueGen, State);
        {error, Reason} ->
            lager:info("Team read failed, error: ~p", [Reason]),
            {error, Reason, State}
    end;

%% Remove a player from the team.
run({team, player, removal}, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Options = [{r,2}, {notfound_ok, true}, {timeout, 5000}],
    Result = riakc_pb_socket:fetch_type(State#state.pid,
                                        State#state.bucket,
                                        Key,
                                        Options),
    case Result of
        {ok, M0} ->
            M = riakc_map:value(M0),
            Members = proplists:get_value({<<"members">>, set}, M, []),
            case length(Members) > 0 of
                true ->
                    Value = hd(Members),
                    M1 = riakc_map:update(
                               {<<"members">>, set},
                               fun(R) ->
                                   riakc_set:del_element(
                                     Value, R)
                               end, M0),
                    Result2 = riakc_pb_socket:update_type(State#state.pid,
                                     State#state.bucket, Key, riakc_map:to_op(M1)),
                    case Result2 of
                        ok ->
                            {ok, State};
                        {ok, _} ->
                            {ok, State};
                        {error, Reason} ->
                            lager:info("Team player removal failed, error: ~p", [Reason]),
                            {error, Reason, State}
                    end;
            false ->
                {ok, State}
            end;
        {error, {notfound, _}} ->
            lager:info("Team does not exist yet."),
            run({team, write}, KeyGen, ValueGen, State);
        {error, Reason} ->
            lager:info("Team read failed, error: ~p", [Reason]),
            {error, Reason, State}
    end;

%% Add a player to the team.
run({team, player, addition}, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Value = "Team member " ++ integer_to_list(ValueGen()),
    Result = riakc_pb_socket:modify_type(State#state.pid,
                     fun(M) ->
                             riakc_map:update(
                                    {<<"members">>, set},
                                    fun(S) ->
                                        riakc_set:add_element(list_to_binary(Value), S)
                                    end, M)
                     end,
                     State#state.bucket, Key, [create]),
    case Result of
        ok ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            lager:info("Team player addition failed, error: ~p", [Reason]),
            {error, Reason, State}
    end;

%% Mark a game as completed.
run({game, completed}, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    Value = ValueGen(),
    Result = riakc_pb_socket:modify_type(State#state.pid,
                     fun(M) ->
                             riakc_map:update(
                                    {<<"score">>, counter},
                                    fun(C) ->
                                        riakc_counter:increment(Value, C)
                                    end, M)
                     end,
                     State#state.bucket, Key, [create]),
    case Result of
        ok ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            lager:info("Score change failed, error: ~p", [Reason]),
            {error, Reason, State}
    end;

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}], State#state.timeout_read) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, disconnected} ->
            run(get, KeyGen, _ValueGen, State);
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
        {error, disconnected} ->
            run(get_existing, KeyGen, _ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Robj = riakc_obj:new(State#state.bucket, KeyGen(), ValueGen(), State#state.content_type),
    case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                     {dw, State#state.dw}], State#state.timeout_write) of
        ok ->
            {ok, State};
        {error, disconnected} ->
            run(put, KeyGen, ValueGen, State);  % suboptimal, but ...
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}], State#state.timeout_read) of
        {ok, Robj} ->
            [M | _] = riakc_obj:get_metadatas(Robj),
            Robj1 = riakc_obj:update_metadata(Robj, M),
            Robj2 = riakc_obj:update_value(Robj1, ValueGen(), State#state.content_type),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}], State#state.timeout_write) of
                ok ->
                    {ok, State};
                {error, disconnected} ->
                    run(update, KeyGen, ValueGen, State);  % suboptimal, but ...
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj = riakc_obj:new(State#state.bucket, Key, ValueGen(), State#state.content_type),
            case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                             {dw, State#state.dw}], State#state.timeout_write) of
                ok ->
                    {ok, State};
                {error, disconnected} ->
                    run(update, KeyGen, ValueGen, State);  % suboptimal, but ...
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, disconnected} ->
            run(update, KeyGen, ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}], State#state.timeout_read) of
        {ok, Robj} ->
            [M | _] = riakc_obj:get_metadatas(Robj),
            Robj1 = riakc_obj:update_metadata(Robj, M),
            Robj2 = riakc_obj:update_value(Robj1, ValueGen(), State#state.content_type),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}], State#state.timeout_write) of
                ok ->
                    {ok, State};
                {error, disconnected} ->
                    run(update_existing, KeyGen, ValueGen, State);  % suboptimal, but ...
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            {error, {not_found, Key}, State};
        {error, disconnected} ->
            run(update_existing, KeyGen, ValueGen, State);
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
        {error, disconnected} ->
            run(delete, KeyGen, _ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete_create, KeyGen, ValueGen, State) ->
    Key = KeyGen(),

    PutResult = maybe_put_key(Key, ValueGen, State),
    DeleteResult = maybe_delete_key(PutResult, Key),
    maybe_put_key(DeleteResult, Key, ValueGen);

run(listkeys, _KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:list_keys(State#state.pid, State#state.bucket, State#state.timeout_listkeys) of
        {ok, _Keys} ->
            {ok, State};
        {error, disconnected} ->
            run(listkeys, _KeyGen, _ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end;
run(search, _KeyGen, _ValueGen, #state{search_queries=SearchQs}=State) ->
    [{Index, Query, Options}|_] = SearchQs,

    NewState = State#state{search_queries=roll_list(SearchQs)},

    case riakc_pb_socket:search(NewState#state.pid, Index, Query, Options, NewState#state.timeout_read) of
          {ok, _Results} ->
              {ok, NewState};
          {error, disconnected} ->
              run(search, _KeyGen, _ValueGen, State);
          {error, Reason} ->
              {error, Reason, NewState}
    end;
run(search_interval, _KeyGen, _ValueGen, #state{search_queries=SearchQs, start_time=StartTime, query_step_interval=Interval}=State) ->
    [{Index, Query, Options}|_] = SearchQs,

    Now = erlang:now(),
    case timer:now_diff(Now, StartTime) of
        _MicroSec when _MicroSec > (Interval * 1000000) ->
            NewState = State#state{search_queries=roll_list(SearchQs),start_time=Now};
        _MicroSec -> 
            NewState = State
    end,

    case riakc_pb_socket:search(NewState#state.pid, Index, Query, Options, NewState#state.timeout_read) of
          {ok, _Results} ->
              {ok, NewState};
          {error, disconnected} ->
              run(search_interval, _KeyGen, _ValueGen, State);
          {error, Reason} ->
              {error, Reason, NewState}
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

run({counter, value}, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    lager:info("Counter value called for key: ~p", [Key]),
    Options = [{r,2}, {notfound_ok, true}, {timeout, 5000}],
    Result = riakc_pb_socket:fetch_type(State#state.pid,
                                        State#state.bucket,
                                        Key,
                                        Options),
    case Result of
        {ok, C0} ->
            C = riakc_counter:value(C0),
            lager:info("Counter value is: ~p", [C]),
            {ok, State};
        {error, {notfound, _}} ->
            {ok, State};
        {error, Reason} ->
            lager:info("Team read failed, error: ~p", [Reason]),
            {error, Reason, State}
    end;

run({counter, increment}, KeyGen, ValueGen, State) ->
    Amt = ValueGen(),
    Key = KeyGen(),
    lager:info("Counter value called for key: ~p", [Key]),
    Result = riakc_pb_socket:modify_type(State#state.pid,
                                         fun(C) ->
                                                 riakc_counter:increment(Amt, C)
                                         end,
                                         State#state.bucket, Key, [create]),
    case Result of
        ok ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            lager:info("Counter increment failed, error: ~p", [Reason]),
            {error, Reason, State}
    end;

run(counter_incr, KeyGen, ValueGen, State) ->
    Amt = ValueGen(),
    Key = KeyGen(),
    case riakc_pb_socket:counter_incr(State#state.pid, State#state.bucket, Key, Amt,
                                      [{w, State#state.w},
                                       {dw, State#state.dw},
                                       {pw, State#state.pw}]) of
        ok ->
            {ok, State};
        {error, disconnected} ->
            run(counter_incr, KeyGen, ValueGen, State);
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
        {error, disconnected} ->
            run(counter_val, KeyGen, _ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================
maybe_delete_key({ok, State}, Key) ->
    Result = case riakc_pb_socket:delete(State#state.pid, State#state.bucket, Key,
                                [{w, all}, {pw, all}, {dw, all}], State#state.timeout_write) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end,
    maybe_durable_get(Result, Key);
maybe_delete_key({error, Reason, State}, _Key) ->
    {error, Reason, State}.

maybe_durable_get({ok, State}, Key) ->
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, all}, {pr, all}], State#state.timeout_read) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
maybe_durable_get({error, Reason, State}, _Key) ->
    {error, Reason, State}.


maybe_put_key({ok, State}, Key, ValueGen) ->
    maybe_put_key(Key, ValueGen, State);
maybe_put_key({error, Reason, State}, _Key, _ValueGen) ->
    {error, Reason, State};
maybe_put_key(Key, ValueGen, State) ->
    Robj = riakc_obj:new(State#state.bucket, Key, ValueGen(), State#state.content_type),
    case riakc_pb_socket:put(State#state.pid, Robj, [{w, all}, {dw, all}], State#state.timeout_write) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

mapred(State, Input, Query) ->
    case riakc_pb_socket:mapred(State#state.pid, Input, Query, State#state.timeout_mapreduce) of
        {ok, Result} ->
            case check_result(State, Input, Query, Result) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, disconnected} ->
            mapred(State, Input, Query);
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

roll_list(List) ->
    [lists:last(List) | lists:sublist(List, length(List) - 1)].

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

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
-module(basho_bench_driver_yz_pbc).

-export([new/1,
         run/4,
         json_valgen/3]).

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

    Ips0  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),

    Ips = case basho_bench_config:get(riakc_pb_hostfile, undefined) of
      undefined -> Ips0;
      Filename -> for_each_line_in_file(
        Filename,
        fun(Entry, Accum) ->
          E = re:replace(Entry, "(^\\s+)|(\\s+$)", "", [global,{return,list}]),
          [E|Accum]
        end,
        [read],
        [])
    end,

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

    %% Choose the target node using our ID as a modulus
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),

    case riakc_pb_socket:start_link(TargetIp, TargetPort, get_connect_options()) of
        {ok, Pid} ->
            %% Hardcode siblings off for these tests
            case riakc_pb_socket:get_bucket(Pid, Bucket) of
              {ok, CurrentProps} ->
                case proplists:get_value(allow_mult, CurrentProps) of
                  false -> ok;
                  _ -> riakc_pb_socket:set_bucket(Pid, Bucket, [{allow_mult, false}])
                end;
              _ -> ok
            end,


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

%% Query 2i results via the PB interface.
run({exact_query_2i, Index, Term, MaxN}, _KeyGen, _ValueGen, State) ->
    Pid = State#state.pid,
    Bucket = State#state.bucket,
    case riakc_pb_socket:get_index_eq(Pid, Bucket, Index, Term,
                      [{timeout, State#state.timeout_general}, {max_results, MaxN}]) of
      {ok, _Results} ->
        {ok, State};
      {error, disconnected} ->
            run({exact_query_2i, Index, Term, MaxN}, _KeyGen, _ValueGen, State);
      {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n", [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end;
run({range_query_2i, Index, StartTerm, EndTerm, MaxN}, _KeyGen, _ValueGen, State) ->
    Pid = State#state.pid,
    Bucket = State#state.bucket,

    case riakc_pb_socket:get_index_range(Pid, Bucket, Index, StartTerm, EndTerm,
                      [{timeout, State#state.timeout_general}, {max_results, MaxN}]) of
      {ok, _Results} ->
        {ok, State};
      {error, disconnected} ->
            run({range_query_2i, Index, StartTerm, EndTerm, MaxN}, _KeyGen, _ValueGen, State);
      {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n", [?MODULE, ?LINE, Reason]),
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
run(put_2i, KeyGen, ValueGen, State) ->
    Obj = riakc_obj:new(State#state.bucket, KeyGen(), ValueGen(), State#state.content_type),
    MD1 = riakc_obj:get_update_metadata(Obj),
    MD2 = riakc_obj:set_secondary_index(MD1, [
      {{binary_index, "tic_obs_z"}, [<<"+tyLAUkc7rF>AnN2i8[luof.w">>]},
      {{binary_index, "n_ipv4_s"}, [<<"155.94.254.143">>]},
      {{binary_index, "m_type_s"}, [<<"tic_calculation">>]},
      {{binary_index, "m_src_s"}, [<<"graph">>]},
      {{integer_index, "m_obs-at_t"}, [1433857067631]},
      {{integer_index, "tic_obs_distance_i"}, [2]},
      {{integer_index, "n_ipv4_i"}, [2606694031]}
      ]),
    Obj2 = riakc_obj:update_metadata(Obj,MD2),
    case riakc_pb_socket:put(State#state.pid, Obj2, [{w, State#state.w},
                        {dw, State#state.dw}], State#state.timeout_write) of
        ok ->
            {ok, State};
        {error, disconnected} ->
            run(put_2i, KeyGen, ValueGen, State);  % suboptimal, but ...
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
roll_list(List) ->
    [lists:last(List) | lists:sublist(List, length(List) - 1)].

json_valgen(_Pid, TemplateFile, ValgenConfig) ->
  {_, Template} = file:read_file(TemplateFile),
  fun() ->
    Data = eval(ValgenConfig, []),
    list_to_binary(io_lib:format(Template, Data))
  end.

eval([], Accum) ->
  lists:reverse(Accum);
eval([{function, {M, F, A}} | Rest], Accum) ->
  eval(Rest, [erlang:apply(M, F, eval(A, [])) | Accum]);
eval([A | Rest], Accum) ->
  eval(Rest, [A | Accum]).

get_timeout_general() ->
    basho_bench_config:get(pb_timeout_general, ?TIMEOUT_GENERAL).

get_timeout(Name) when Name == pb_timeout_read;
                       Name == pb_timeout_write;
                       Name == pb_timeout_listkeys;
                       Name == pb_timeout_mapreduce ->
    basho_bench_config:get(Name, get_timeout_general()).

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).

for_each_line_in_file(Name, Proc, Mode, Accum0) ->
    {ok, Device} = file:open(Name, Mode),
    for_each_line(Device, Proc, Accum0).

for_each_line(Device, Proc, Accum) ->
    case io:get_line(Device, "") of
        eof  ->
            file:close(Device), Accum;
        Line ->
            NewAccum = Proc(Line, Accum),
            for_each_line(Device, Proc, NewAccum)
    end.

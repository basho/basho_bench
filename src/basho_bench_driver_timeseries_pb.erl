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
-module(basho_bench_driver_timeseries_pb).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 ts,
                 id,
                 timestamp,
                 hostname,
                 batch_size,
                 families,
                 series,
                 templ,
                 f1,
                 f2,
                 f3
               }).

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
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"GeoCheckin">>),
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    Ts = basho_bench_config:get(ts, true),
    BatchSize = basho_bench_config:get(batch_size, 1),
    Families = basho_bench_config:get(riakts_families, ["Test"]),
    Series = basho_bench_config:get(riakts_series, ["Test"]),
    F1 = basho_bench_config:get(riakts_f1, ["Test"]),
    F2 = basho_bench_config:get(riakts_f2, ["Test"]),
    F3 = basho_bench_config:get(riakts_f3, ["Test"]),
    Templ = basho_bench_config:get(riakts_templ, "~p~p~p~p~p~p~p~p~p~p"),
    {Mega,Sec,Micro} = erlang:now(),
    NowTimestamp = (Mega*1000000 + Sec)*1000 + round(Micro/1000),
    Timestamp = basho_bench_config:get(start_timestamp, NowTimestamp),
    io:format("Worker ~p Starting Timestamp: ~p~n", [Id, Timestamp]),
    {ok, Hostname} = inet:gethostname(),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
     ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort) of
        {ok, Pid} ->
            riakc_pb_socket:use_native_encoding(Pid, true),
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          ts = Ts,
                          id = list_to_binary(lists:flatten(io_lib:format("~p", [Id]))),
                          timestamp = Timestamp,
                          hostname = list_to_binary(Hostname),
                          batch_size = BatchSize,
                          families = Families,
                          series = Series,
                          templ = Templ,
                          f1 = F1,
                          f2 = F2,
                          f3 = F3
            }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

run(put, KeyGen, ValueGen, State) ->
    Robj = riakc_obj:new(State#state.bucket, KeyGen(), ValueGen()),
    case riakc_pb_socket:put(State#state.pid, Robj) of
        ok ->
            {ok, State};
        {error, disconnected} ->
            run(put, KeyGen, ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
	end;

run(fast_put_pb, KeyGen, ValueGen, State) ->
    Pid = State#state.pid,
    Bucket = State#state.bucket,
    Key = KeyGen(),
    Value = ValueGen(),

    %% Create the object...
    Robj0 = riakc_obj:new(Bucket, Key),
    Robj1 = riakc_obj:update_value(Robj0, Value),

    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj1) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

  run(ts_sequential, _KeyGen, _ValueGen, State) ->
    Pid = State#state.pid,
    Timestamp = State#state.timestamp,
    Bucket = State#state.bucket,
    BatchSize = State#state.batch_size,

    Families = State#state.families,
    Series = State#state.series,
    F1 = State#state.f1,
    F2 = State#state.f2,
    F3 = State#state.f3,
    Templ = State#state.templ,

    Val = lists:map(fun (X) ->
                            [list_to_binary(lists:nth(random:uniform(length(Families)), Families)),
                             list_to_binary(lists:nth(random:uniform(length(Series)), Series)),
                             Timestamp + (X-1),
                             list_to_binary(lists:nth(random:uniform(length(F1)), F1)),
                             list_to_binary(lists:nth(random:uniform(length(F2)), F2)),
                             list_to_binary(lists:nth(random:uniform(length(F3)), F3)),
                             ts_templ(Templ)]
                    end,
                    lists:seq(1,BatchSize)),

    case State#state.ts of
        true ->
            case riakc_ts:put(Pid, Bucket, Val) of
                ok ->
                    {ok, State#state{timestamp = Timestamp + BatchSize}};
                {error, Reason} ->
                    {error, Reason, State}
            end;

        false ->
            [[Fam, Ser, _, _, _, _,Data]] = Val,
            Key = <<(Fam)/binary,
                    (Ser)/binary,
                    (list_to_binary(integer_to_list(Timestamp)))/binary>>,
            Obj = riakc_obj:new(Bucket, Key, Data),
            case riakc_pb_socket:put(Pid, Obj) of
                {ok, _} ->
                    {ok, State#state{timestamp = Timestamp + BatchSize}};
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;

run(ts_get, KeyGen, _ValueGen, State) ->
    Pid = State#state.pid,
    Key = KeyGen(),

    case State#state.ts of
        true ->
            case riakc_ts:get(Pid, State#state.bucket, Key, []) of
                {error, Reason} ->
                    {error, Reason, State};
                {ok, _Results} ->
                    {ok, State}
            end;
        false ->
            [Family, Series, Time] = Key,
            BinKey = <<(Family)/binary,
                       (Series)/binary,
                       (list_to_binary(integer_to_list(Time)))/binary>>,
            case riakc_pb_socket:get(Pid, State#state.bucket, BinKey) of
                {ok, _} ->
                    {ok, State};
                {error, notfound} ->
                    {ok, State};
                {error, disconnected} ->
                    run(get, KeyGen, _ValueGen, State);
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;

run(ts_query, KeyGen, _ValueGen, State) ->
    Pid = State#state.pid,
    Query = KeyGen(),

    case riakc_ts:query(Pid, Query) of
        {error, Reason} ->
            {error, Reason, State};
        {_Schema, _Results} ->
            {ok, State}
    end.

ts_templ(Templ) ->
    list_to_binary(io_lib:format(Templ, [
                                         random:uniform(100),
                                         random:uniform(100),
                                         random:uniform(100),
                                         random:uniform(100),
                                         random:uniform(100),
                                         random:uniform(100),
                                         random:uniform(100),
                                         random:uniform(100),
                                         random:uniform(100)
                         ])).

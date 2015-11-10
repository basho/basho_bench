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
                 id,
                 bucket,
                 timestamp,
                 hostname
               }).

-define(BATCH_SIZE, 250).

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    random:seed(now()),

    Ips  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    {Mega,Sec,Micro} = erlang:now(),
    Timestamp = (Mega*1000000 + Sec)*1000 + round(Micro/1000),
    {ok, Hostname} = inet:gethostname(),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort) of
        {ok, Pid} ->
            io:format("Worker PID: ~p~n", [Pid]),
            {ok, #state { pid = Pid,
                          id = Id,
                          bucket = Bucket,
                          timestamp = Timestamp,
                          hostname = list_to_binary(Hostname)
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

run(put_ts, KeyGen, _ValueGen, State) ->
  _Key = KeyGen(),
  Timestamp = State#state.timestamp,
  Val =  lists:map(fun (X) -> [{time, Timestamp + (X-1)}, State#state.id, State#state.hostname, 100.0, 50.5] end, lists:seq(1,?BATCH_SIZE)),
  State#state{timestamp = Timestamp + ?BATCH_SIZE},
  case riakc_ts:put(State#state.pid, State#state.bucket, Val) of
    ok ->
      {ok, State#state {timestamp = Timestamp + ?BATCH_SIZE}};
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
    end.

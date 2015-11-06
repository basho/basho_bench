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
                 ts
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
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
     ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          ts = Ts
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

run(put_ts, KeyGen, ValueGen, State) ->
  _Key = KeyGen(),
  case riakc_ts:put(State#state.pid, State#state.bucket, ValueGen()) of
    ok ->
      {ok, State};
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

run(rt_port_ts, KeyGen, ValueGen, State) ->
  Pid = State#state.pid,
  Bucket = State#state.bucket,
  Data = ValueGen(),
  Key = KeyGen(),
  case State#state.ts of
    true ->
      case riakc_ts:put(Pid, Bucket, ValueGen()) of
        ok ->
          {ok, State};
        {error, Reason} ->
          {error, Reason, State}
        end;
    
    false ->
      Obj = riakc_obj:new({<<"fastpath">>,<<"fastpath">>}, list_to_binary(integer_to_list(Key)), Data),
      case riakc_pb_socket:put(Pid, Obj) of
        ok ->
          {ok, State};
        {error, Reason} ->
          {error, Reason, State}
        end
  end.
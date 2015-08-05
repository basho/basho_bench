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
-module(basho_bench_driver_redis).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { c, bucket }).

new(Id) ->
	Ips  = basho_bench_config:get(redis_ips, ["127.0.0.1"]),
	Port  = basho_bench_config:get(redis_port, 6379),
	Targets = basho_bench_config:normalize_ips(Ips, Port),
	Bucket  = basho_bench_config:get(bucket_name, "my-bucket"),
	{TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
	?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),

	case eredis:start_link(TargetIp, TargetPort) of
		{ok, C} ->
			{ok, #state { 
				c = C, 
				bucket = Bucket
			}};
		{error, Reason} ->
			?FAIL_MSG("Failed to connect to redis at ~p:~p: ~p\n", [TargetIp, TargetPort, Reason])
	end.

run(put, KeyGen, ValueGen, State) ->
	case eredis:q(State#state.c, ["SET", KeyGen(), ValueGen()]) of
		{ok, _} -> 
			{ok, State};
		{error, Reason} -> 
			{error, Reason, State}
	end;
run(get, KeyGen, _ValueGen, State) ->
	KeyNum = KeyGen(),
	Key = lists:flatten(State#state.bucket ++ io_lib:format(":~p", [KeyNum])),
	io:format("Key ~p~n", [Key]), 
	case eredis:q(State#state.c, ["GET", Key]) of
		{ok, _} ->
			{ok, State};
		{error, Reason} ->
			{error, Reason, State}
	end.
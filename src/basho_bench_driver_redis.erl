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

-record(state, { c, bucket, retries, retry_delay }).

new(Id) ->
	Ips  = basho_bench_config:get(redis_ips, ["127.0.0.1"]),
	Port  = basho_bench_config:get(redis_port, 6379),
    Retries = basho_bench_config:get(retries, 3),
    RetryDelay = basho_bench_config:get(retry_delay, 10),
	Targets = basho_bench_config:normalize_ips(Ips, Port),
	Bucket  = basho_bench_config:get(bucket_name, "my-bucket"),
	{TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
	?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),

	case eredis:start_link(TargetIp, TargetPort) of
		{ok, C} ->
			{ok, #state { 
                    c = C,
                    bucket = Bucket,
                    retries = Retries,
                    retry_delay = RetryDelay
			}};
		{error, Reason} ->
			?FAIL_MSG("Failed to connect to redis at ~p:~p: ~p\n", [TargetIp, TargetPort, Reason])
	end.

get_key(KeyGen, #state{bucket = Bucket}) -> 
    KeyNum = integer_to_list(KeyGen()),
    lists:flatten([Bucket, ":", KeyNum]).

execute_w_retries(Connection, Command, State, 0) ->
    case eredis:q(Connection, Command) of
        {ok, _} -> {ok, State};
        {error, Reason} -> {error, Reason, State}
    end;
execute_w_retries(Connection, Command, State = #state{retry_delay = RetryDelay},
                  Retries) ->
    case eredis:q(Connection, Command) of
        {ok, _} -> {ok, State};
        {error, _Reason} ->
            timer:sleep(RetryDelay),
            execute_w_retries(Connection, Command, State, Retries - 1)
    end.

run(set, KeyGen, ValueGen, State) ->
    Key = get_key(KeyGen, State),
    execute_w_retries(State#state.c, ["SET", Key, ValueGen()], State,
                      State#state.retries);
run(del, KeyGen, _ValueGen, State) ->
    Key = get_key(KeyGen, State),
    execute_w_retries(State#state.c, ["DEL", Key], State,
                      State#state.retries);
run(get, KeyGen, _ValueGen, State) ->
    Key = get_key(KeyGen, State),
    execute_w_retries(State#state.c, ["GET", Key], State,
                      State#state.retries);
run(invalid, KeyGen, _ValueGen, State) ->
    Keys = [get_key(KeyGen, State), get_key(KeyGen, State)],
    case eredis:q(State#state.c, ["INVALID", Keys]) of
        {ok, _} ->
            {error, "Invalid command succeeded", State};
        {error, _Reason} ->
            {ok, State}
    end.

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
-module(basho_bench_driver_yz_timeseries).

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
                 random_values,
                 index_count
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

    % Seed the RNG
    random:seed(now()),

    Ips  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, {<<"GeoCheckin">>, <<"GeoCheckin">>}),
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    Ts = basho_bench_config:get(ts, true),
    RandomValues = basho_bench_config:get(random_values, true),
    BatchSize = basho_bench_config:get(batch_size, 1),
    {Mega,Sec,Micro} = erlang:now(),
    NowTimestamp = (Mega*1000000 + Sec)*1000 + round(Micro/1000),
    Timestamp = basho_bench_config:get(start_timestamp, NowTimestamp),
    IndexCount = basho_bench_config:get(index_count, 5),
    io:format("Worker ~p Starting Timestamp: ~p~n", [Id, Timestamp]),
    {ok, Hostname} = inet:gethostname(),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
     ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          ts = Ts,
                          id = list_to_binary(lists:flatten(io_lib:format("~p", [Id]))),
                          timestamp = Timestamp,
                          hostname = list_to_binary(Hostname),
                          batch_size = BatchSize,
                          random_values = RandomValues,
                          index_count = IndexCount
            }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

  run(put, _KeyGen, _ValueGen, State) ->
    Pid = State#state.pid,
    _Bucket = State#state.bucket,

    Timestamp = State#state.timestamp,
    Hostname = State#state.hostname,
    Id = State#state.id,
    IndexCount = State#state.index_count,

    RandomValues = State#state.random_values,

    {Key, Data} = get_kv(RandomValues, Hostname, Id, Timestamp),

    BucketIndex = random:uniform(IndexCount) - 1,
    RndBucketName = list_to_binary(io_lib:format("GeoCheckin~p", [BucketIndex])),
    RndBucket = {RndBucketName, RndBucketName},
    %io:format("Bucket: ~p~n", [RndBucket]),

    Obj = riakc_obj:new(RndBucket, Key, Data, <<"application/json">>), 
    %io:format("~p~n", [Obj]),

    case riakc_pb_socket:put(Pid, Obj) of
      ok ->
        {ok, State#state{timestamp = Timestamp+1}};
      {error, Reason} ->
        io:format("Error: ~p~n", [Reason]),
        {error, Reason, State}
    end;

  run(get, KeyGen, _ValueGen, State) ->
    Pid = State#state.pid,
     IndexCount = State#state.index_count,
    
    BucketIndex = random:uniform(IndexCount) - 1,
    Bucket = list_to_binary(io_lib:format("time~p", [BucketIndex])),
    Query = KeyGen(),
    {ok, Results} = riakc_pb_socket:search(Pid, Bucket, Query),
    %io:format("~p~n", [Results]),
    {search_results, _, _, Count} = Results,
    %io:format("Count: ~p~n", [Count]),
    {ok, State}.
  
get_kv(RandomValues, Hostname, Id, Timestamp) ->
  if
    RandomValues ->
      MyInt = random:uniform(100),
      MyString = list_to_binary(lists:foldl(fun(_, Str) -> [random:uniform(26) + 96 | Str] end, [], lists:seq(1,10))),
      MyDouble = random:uniform() * 100,
      MyBool = lists:nth(random:uniform(2), [true, false]),

      Key = iolist_to_binary(io_lib:format("~s-~s-~p", [Hostname, Id, Timestamp])),
      D = {struct, [{family, Hostname}, {series, Id}, {time, Timestamp}, {myint, MyInt}, {mystring, MyString}, {mydouble, MyDouble}, {mybool, MyBool}]},
      JD = iolist_to_binary(mochijson2:encode(D)),
      {Key, JD};
    true ->
      MyInt = 123,
      MyString = <<"test1">>,
      MyDouble = 1.5,
      MyBool = true,

      Key = iolist_to_binary(io_lib:format("~s-~s-~p", [Hostname, Id, Timestamp])),
      D = {struct, [{family, Hostname}, {series, Id}, {time, Timestamp}, {myint, MyInt}, {mystring, MyString}, {mydouble, MyDouble}, {mybool, MyBool}]},
      JD = iolist_to_binary(mochijson2:encode(D)),
      {Key, JD}
  end.
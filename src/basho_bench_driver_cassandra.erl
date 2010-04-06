%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(basho_bench_driver_cassandra).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").
-include_lib("casbench/include/cassandra_thrift.hrl").

-record(state, { client,
                 keyspace,
                 colpath }).


%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(cassandra_thrift) of
        non_existing ->
            ?FAIL_MSG("~s requires cassandra_thrift module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Hosts    = basho_bench_config:get(cassandra_hosts, ["localhost"]),
    Port     = basho_bench_config:get(cassandra_port, 9160),
    Keyspace = basho_bench_config:get(cassandra_keyspace, "Keyspace1"),
    ColPath  = #columnPath { column_family = "Standard1", column = "col1" },

    %% Choose the node using our ID as a modulus
    TargetHost = lists:nth((Id rem length(Hosts)+1), Hosts),
    ?INFO("Using target ~s:~p for worker ~p\n", [TargetHost, Port, Id]),

    case thrift_client:start_link(TargetHost, Port, cassandra_thrift) of
        {ok, Client} ->
            {ok, #state { client = Client,
                          keyspace = Keyspace,
                          colpath = ColPath }};
        {error, Reason} ->
            ?FAIL_MSG("Failed to get a thrift_client for ~p: ~p\n", [TargetHost, Reason])
    end.

call(State, Op, Args) ->
    (catch thrift_client:call(State#state.client, Op, Args)).

tstamp() ->
    {Mega, Sec, _Micro} = now(),
    (Mega * 1000000) + Sec.


run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case call(State, get, [State#state.keyspace, Key, State#state.colpath, 1]) of
        {ok, _} ->
            {ok, State};
        {notFoundException} ->
            {ok, State};
        {'EXIT', {timeout, _}} ->
            {error, timeout, State};
        Error ->
            {error, Error, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    case call(State, insert, [State#state.keyspace, KeyGen(), State#state.colpath,
                              ValueGen(), tstamp(), 1]) of
        {ok, ok} ->
            {ok, State};
        {'EXIT', {timeout, _}} ->
            {error, timeout, State};
        Error ->
            {error, Error, State}
    end.

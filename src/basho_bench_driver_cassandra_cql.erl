%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2012 Basho Techonologies
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
-module(basho_bench_driver_cassandra_cql).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").
-include("deps/cqerl/include/cqerl.hrl").

-record(state, { client,
                 keyspace,
                 columnfamily,
                 column,
                 get_query,
                 insert_query,
                 put_query,
                 delete_query
               }).


%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    Ips = basho_bench_config:get(cassandra_ips, [{"localhost", 9042}]),
    Port = basho_bench_config:get(cassandra_port, 9042),
    Keyspace = basho_bench_config:get(cassandra_keyspace, "Keyspace1"),
    ColumnFamily = basho_bench_config:get(cassandra_columnfamily, "ColumnFamily1"),
    Column = basho_bench_config:get(cassandra_column, "Column"),
    ReadConsistency = basho_bench_config:get(cassandra_read_consistency, ?CQERL_CONSISTENCY_ONE),
    WriteConsistency = basho_bench_config:get(cassandra_write_consistency, ?CQERL_CONSISTENCY_THREE),
    %% connect to client
        %% Choose the target node using our ID as a modulus
        Targets = basho_bench_config:normalize_ips(Ips, Port),
        {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
        ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
        application:ensure_all_started(cqerl),
        {ok, C} = cqerl:new_client({TargetIp, TargetPort}),

    case ksbarrier(C, Keyspace) of
        ok ->
            %% Build parameterized, reusable queries as we assume a typical
            %% high-volume Cassandra application would.
            GetQueryText = iolist_to_binary(["SELECT ", Column ," FROM ", ColumnFamily ," where KEY = :key"]),
            GetQuery = #cql_query{statement = GetQueryText, consistency = ReadConsistency},
            InsertQueryText = iolist_to_binary(["INSERT INTO ", ColumnFamily ,
                " (KEY, ", Column, ") VALUES "
                "(:key, :val);"]),
            InsertQuery = #cql_query{statement = InsertQueryText, consistency = WriteConsistency},
            PutQueryText = iolist_to_binary(["UPDATE ", ColumnFamily,
                " SET ", Column, " = :val WHERE KEY = :key;"]),
            PutQuery = #cql_query{statement = PutQueryText, consistency = WriteConsistency},
            DeleteQueryText = ["DELETE FROM ", ColumnFamily ," WHERE KEY = :key;"],
            DeleteQuery = #cql_query{statement = DeleteQueryText, consistency = WriteConsistency},

			{ok, #state { client = C,
						  keyspace = Keyspace,
						  columnfamily = ColumnFamily,
						  column = Column,
                          get_query = GetQuery,
                          insert_query = InsertQuery,
                          put_query = PutQuery,
                          delete_query = DeleteQuery}};
        {error, Reason} ->
			error_logger:error_msg("Failed to get a cqerl client for ~p: ~p\n",
                                   [TargetIp, Reason])
    end.


ksbarrier(C, Keyspace) ->
    case cqerl:run_query(C, lists:concat(["USE ", Keyspace, ";"])) of
		{ok, _KSBin} -> ok;
        {error, not_ready} ->
            %% Not ready yet, try again
            timer:sleep(100),
            ksbarrier(C, Keyspace);
		{error, _} = Error ->
            Error
	end.

run(get, KeyGen, _ValueGen,
	#state{client=C, get_query=CqlQuery}=State) ->
	Key = KeyGen(),
    ParameterizedQuery = CqlQuery#cql_query{values = [{key, Key}]},
    case cqerl:run_query(C, ParameterizedQuery) of
        {ok, #cql_result{cql_query=ParameterizedQuery} = _Result} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(insert, KeyGen, ValueGen,
    #state{client=C, insert_query=InsertQuery}=State) ->
    Key = KeyGen(),
    Val = ValueGen(),
    ParameterizedQuery = InsertQuery#cql_query{values = [{key, Key}, {val, Val}]},

    case cqerl:run_query(C, ParameterizedQuery) of
        {ok,void} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(put, KeyGen, ValueGen,
	#state{client=C, put_query = PutQuery}=State) ->
	Key = KeyGen(),
	Val = ValueGen(),
	ParameterizedQuery = PutQuery#cql_query{values = [{key, Key}, {val, Val}]},

    case cqerl:run_query(C, ParameterizedQuery) of
        {ok,void} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(delete, KeyGen, _ValueGen,
    #state{client=C, delete_query=DeleteQuery}=State) ->
	Key = KeyGen(),
    ParameterizedQuery = DeleteQuery#cql_query{values = [{key, Key}]},
    case cqerl:run_query(C, ParameterizedQuery) of
        {ok,void} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end.

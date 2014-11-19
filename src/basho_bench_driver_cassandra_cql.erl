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
                 column
               }).


%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    Host    = basho_bench_config:get(cassandra_host, "localhost"),
    Port     = basho_bench_config:get(cassandra_port, 9042),
    Keyspace = basho_bench_config:get(cassandra_keyspace, "Keyspace1"),
    ColumnFamily = basho_bench_config:get(cassandra_columnfamily, "ColumnFamily1"),
    Column = basho_bench_config:get(cassandra_column, "Column"),

    %% connect to client
    application:ensure_all_started(cqerl),
    {ok, C} = cqerl:new_client({Host, Port}),
    error_logger:info_msg("Id: ~p, "
        "Connected to Cassandra at Host ~p and Port ~p\n", [Id, Host, Port]),


    case ksbarrier(C, Keyspace) of
        ok ->
			{ok, #state { client = C,
						  keyspace = Keyspace,
						  columnfamily = ColumnFamily,
						  column = Column}};
        {error, Reason} ->
			error_logger:error_msg("Failed to get a cqerl client for ~p: ~p\n",
                                   [Host, Reason])
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
	#state{client=C, columnfamily=ColumnFamily, column=Column}=State) ->
	Key = KeyGen(),
	Query = ["SELECT ", Column ," FROM ", ColumnFamily ," where KEY = '", Key ,"';"],
    case cqerl:run_query(C, #cql_query{statement = Query,
                                       consistency = ?CQERL_CONSISTENCY_ONE}) of
        {ok,void} ->
            {ok, State};
        {ok, {_Rows, _Cols}} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(insert, KeyGen, ValueGen,
    #state{client=C, columnfamily=ColumnFamily, column=Column}=State) ->
    Key = KeyGen(),
    Val = ValueGen(),
    Query = ["INSERT INTO ", ColumnFamily ,
               " (KEY, ", Column, ") VALUES "
               "('", Key ,"', ", bin_to_hexstr(Val) ,");"],

    case cqerl:run_query(C, #cql_query{statement = Query,
                                       consistency = ?CQERL_CONSISTENCY_ANY}) of
        {ok,void} ->
            {ok, State};
        {ok, {_Rows, _Cols}} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(put, KeyGen, ValueGen,
	#state{client=C, columnfamily=ColumnFamily, column=Column}=State) ->
	Key = KeyGen(),
	Val = ValueGen(),
	Query = ["UPDATE ", ColumnFamily,
             " SET ", Column, " = ", bin_to_hexstr(Val),
             " WHERE KEY = '", Key, "';"],

    case cqerl:run_query(C, #cql_query{statement = Query,
                                       consistency = ?CQERL_CONSISTENCY_ANY}) of
        {ok,void} ->
            {ok, State};
		{ok, {_Rows, _Cols}} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(delete, KeyGen, _ValueGen,
    #state{client=C, columnfamily=ColumnFamily}=State) ->
	Key = KeyGen(),
	Query = ["DELETE FROM ", ColumnFamily ," WHERE KEY = '", Key ,"';"],
    case cqerl:run_query(C, #cql_query{statement = Query,
                                       consistency = ?CQERL_CONSISTENCY_ANY}) of
        {ok,void} ->
            {ok, State};
        {ok, {_Rows, _Cols}} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end.

%% Internal Functions

hex(N) when N < 10 ->
    $0+N;
hex(N) when N >= 10, N < 16 ->
    $a+(N-10).

bin_to_hexstr(Bin) ->
    List = binary_to_list(Bin),
    ["0x", [ [hex(N div 16), hex(N rem 16)] || N <- List, N < 256 ] ].

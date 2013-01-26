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

-compile({parse_transform, basho_bench_provide}).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").
-include_lib("erlcassa/include/erlcassa.hrl").

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
    Port     = basho_bench_config:get(cassandra_port, 9160),
    Keyspace = basho_bench_config:get(cassandra_keyspace, "Keyspace1"),
	ColumnFamily = basho_bench_config:get(cassandra_columnfamily, "ColumnFamily1"),
	Column = basho_bench_config:get(cassandra_column, "Column"),

	% connect to client
	{ok, C} = erlcassa_client:connect(Host, Port),
	?INFO("Id: ~p, Connected to Cassandra at Host ~p and Port ~p\n", [Id, Host, Port]),
	
	% use keyspace
	{result, ok} = erlcassa_client:cql_execute(C, lists:concat(["USE ", Keyspace, ";"])), 

	case erlcassa_client:cql_execute(C, lists:concat(["USE ", Keyspace, ";"])) of
		{result, ok} ->
			{ok, #state { client = C,
						  keyspace = Keyspace,
						  columnfamily = ColumnFamily,
						  column = Column}};
		{error, Reason} -> 
			?FAIL_MSG("Failed to get a thrift_client for ~p: ~p\n", [Host, Reason])
	end.

run(get, KeyGen, _ValueGen, 
	#state{client=C, columnfamily=ColumnFamily, column=Column}=State) ->
	Key = KeyGen(),
	Query = lists:concat(["SELECT ", Column ," FROM ", ColumnFamily ," where KEY = '", Key ,"';"]), 
	case erlcassa_client:cql_execute(C, Query, proplist) of
        {result, {rows, _Rows}} ->
			%% [Row|_] = Rows,
			%% KeyColumn = erlcassa_client:get_column("KEY", Row),
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(insert, KeyGen, ValueGen,
    #state{client=C, columnfamily=ColumnFamily, column=Column}=State) ->
    Key = KeyGen(),
    Val = ValueGen(),
    Query = lists:concat(["INSERT INTO ", ColumnFamily , " (KEY, ", Column, ") VALUES ('", Key ,"', ", bin_to_hexstr(Val) ,");"]),
	case erlcassa_client:cql_execute(C, Query) of
        {result, ok} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(put, KeyGen, ValueGen,
	#state{client=C, columnfamily=ColumnFamily, column=Column}=State) ->
	Key = KeyGen(),
	Val = ValueGen(),
	Query = lists:concat(["UPDATE ", ColumnFamily, " SET '", Column, "' = ", bin_to_hexstr(Val), " WHERE KEY = '", Key, "';"]),
	case erlcassa_client:cql_execute(C, Query, proplist) of
		{result,ok} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(delete, KeyGen, _ValueGen,
    #state{client=C, columnfamily=ColumnFamily}=State) ->
	Key = KeyGen(),
	Query = lists:concat(["DELETE FROM ", ColumnFamily ," where KEY = '", Key ,"';"]),
	case erlcassa_client:cql_execute(C, Query) of
        {result, ok} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end.

%% Internal Functions

hex(N) when N < 10 ->
    $0+N;
hex(N) when N >= 10, N < 16 ->
    $a+(N-10).
    
to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].
 
list_to_hexstr([]) -> 
    [];
list_to_hexstr([H|T]) ->
    to_hex(H) ++ list_to_hexstr(T).

bin_to_hexstr(Bin) ->
    lists:concat(["abcdef0123",list_to_hexstr(binary_to_list(Bin))]).

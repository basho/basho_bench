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
-module(basho_bench_driver_cs).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {bucket}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
	%% Ensure that erlcloud is up
	erlcloud:start(),

	%% Configure erlcloud
	AccessKey = basho_bench_config:get(cs_access_key),
	SecretKey = basho_bench_config:get(cs_secret_key),
	Host      = basho_bench_config:get(cs_host, "localhost"),
	Port      = basho_bench_config:get(cs_port,  8080),
	Protocol  = basho_bench_config:get(cs_protocol, "https"),
	Bucket    = basho_bench_config:get(cs_bucket, "bench_test"),

	erlcloud_s3:configure(AccessKey, SecretKey, Host, Port, Protocol),
	erlcloud_s3:create_bucket(Bucket),

	{ok, #state {bucket=Bucket}}.


run(get, KeyGen, _ValueGen, State) ->
	try
		Key = KeyGen(),
		erlcloud_s3:get_object(State#state.bucket, integer_to_list(Key)),
    	{ok, State}
	catch _X:_Y ->
%%        ?ERROR("Error on ~p: ~p ~p\n", [Key, _X, _Y]),
		{ok, State}
	end;

run(put, KeyGen, ValueGen, State) ->
	Key = KeyGen(),
	Value = ValueGen(),
	erlcloud_s3:put_object(State#state.bucket, integer_to_list(Key), Value),
	{ok, State};
run(delete, KeyGen, _ValueGen, State) ->
	_Key = KeyGen(),
	{ok, State}.


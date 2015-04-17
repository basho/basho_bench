%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Techonologies
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

%% Raw eleveldb driver. It opens a number of eleveldb instances and assigns
%% one to each created worker in round robin fashion. So, for example, creating
%% 32 instances and 64 concurrent workers would bind a pair of workers to 
%% each instance for all operations.
-module(basho_bench_driver_eleveldb).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {
          instance
         }).

get_instances() ->
    case basho_bench_config:get(eleveldb_instances, undefined) of
        undefined ->
            Instances = start_instances(),
            basho_bench_config:set(eleveldb_instances, Instances),
            Instances;
        Instances ->
            Instances
    end.


start_instances() ->
    case code:which(eleveldb) of
        non_existing ->
            ?FAIL_MSG("~s requires eleveldb to be avaiable on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,
    BaseDir = basho_bench_config:get(eleveldb_dir, "."),
    Num = basho_bench_config:get(eleveldb_num_instances, 1),
    Config = basho_bench_config:get(eleveldb_config, [{create_if_missing, true}]),
    ?INFO("Starting up ~p eleveldb instances under ~s with config ~p.\n",
          [Num, BaseDir, Config]),
    Refs = [begin
                Dir = filename:join(BaseDir, "instance." ++ integer_to_list(N)),
                ?INFO("Opening eleveldb instance in ~s\n", [Dir]),
                {ok, Ref} = eleveldb:open(Dir, Config),
                Ref
            end || N <- lists:seq(1, Num)],
    list_to_tuple(Refs).

new(Id) ->
    Instances = get_instances(),
    Count = size(Instances),
    Idx = ((Id - 1) rem Count) + 1,
    ?INFO("Worker ~p using instance ~p.\n", [Id, Idx]),
    State = #state{instance = element(Idx, Instances)},
    {ok, State}.


run(get, KeyGen, _ValueGen, State = #state{instance = Ref}) ->
    Key = KeyGen(),
    case eleveldb:get(Ref, Key, []) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValGen, State = #state{instance = Ref}) ->
    Key = KeyGen(),
    Value = ValGen(),
    case eleveldb:put(Ref, Key, Value, []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.



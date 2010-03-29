%% -------------------------------------------------------------------
%%
%% riak_bench: Benchmarking Suite for Riak
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
-module(riak_bench_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         workers/0]).

%% Supervisor callbacks
-export([init/1]).

-include("riak_bench.hrl").

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

workers() ->
    [Pid || {_Id, Pid, worker, [riak_bench_worker]} <- supervisor:which_children(?MODULE)].


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    %% Get the number concurrent workers we're expecting and generate child
    %% specs for each
    Workers = worker_specs(riak_bench_config:get(concurrent), []),
    {ok, {{one_for_one, 5, 10}, [?CHILD(riak_bench_log, worker),
                                 ?CHILD(riak_bench_stats, worker)] ++ Workers}}.


%% ===================================================================
%% Internal functions
%% ===================================================================

worker_specs(0, Acc) ->
    Acc;
worker_specs(Count, Acc) ->
    Id = list_to_atom(lists:concat(['riak_bench_worker_', Count])),
    Spec = {Id, {riak_bench_worker, start_link, [Count]},
            transient, 5000, worker, [riak_bench_worker]},
    worker_specs(Count-1, [Spec | Acc]).

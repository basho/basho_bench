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
-module(basho_bench_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         workers/0,
         stop_child/1,
         active_workers/0]).

%% Supervisor callbacks
-export([init/1]).

-include("basho_bench.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

workers() ->
    [Pid || {_Id, Pid, worker, [basho_bench_worker]} <- supervisor:which_children(?MODULE)].

stop_child(Id) ->
    supervisor:terminate_child(?MODULE, Id).

active_workers() ->
    [X || X <- workers(), X =/= undefined].


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    %% Get the number concurrent workers we're expecting and generate child
    %% specs for each

    basho_bench_profiler:maybe_start_profiler(basho_bench_config:get(enable_profiler, false)),

    WorkerSpecs = worker_specs(basho_bench_config:get(workers, [])),
    {ok, {{one_for_one, 5, 10}, WorkerSpecs}}.


%% ===================================================================
%% Internal functions
%% ===================================================================


worker_specs([]) ->
    worker_specs_single(basho_bench_config:get(concurrent), []);
worker_specs(Workers) ->
    WorkerConfs = combine_multi_confs(Workers, basho_bench_config:get(worker_types)),
    %% Need to make sure worker 1 within a worker type is started first
    lists:reverse(worker_specs_multi(WorkerConfs, 0, [])).


worker_specs_single(0, Acc) ->
    Acc;
worker_specs_single(Count, Acc) ->
    Id = list_to_atom(lists:concat(['basho_bench_worker_', Count])),
    %% Use "single_worker" atom for original non-worker case
    Spec = {Id, {basho_bench_worker, start_link, [Id, {single_worker, Count, Count}, []]},
                 transient, 5000, worker, [basho_bench_worker]},
    worker_specs_single(Count-1, [Spec | Acc]).


worker_specs_multi([], _BaseGlobalId, Acc) ->
    Acc;
worker_specs_multi([{WorkerType, Count, Conf} | Rest], BaseGlobalId, Acc0) ->
    Acc = lists:foldl(
       fun(I, AccP) ->
            Id = list_to_atom(lists:concat(['basho_bench_worker_', WorkerType, '_', I])),
            Spec = {
                Id,
                {basho_bench_worker, start_link, [Id, {WorkerType, I, I+BaseGlobalId}, Conf]},
                transient, 5000, worker, [basho_bench_worker]},
            [Spec | AccP]
        end,
        Acc0, lists:seq(1, Count)),
    worker_specs_multi(Rest, BaseGlobalId+Count, Acc).


combine_multi_confs(Workers, WorkerTypes) ->
    lists:map(
        fun({WT, Count}) ->
            %% Burn in {concurrent, Count} to the WorkerConf
            %% Keygen(sequential) needs number of workers sharing same keygen
            Conf0 = proplists:get_value(WT, WorkerTypes, []),
            Conf = [{concurrent, Count} | proplists:delete(concurrent, Conf0)],
            {WT, Count, Conf}
        end, Workers).

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

    case basho_bench_config:get(enable_eprof, false) of 
        false ->
            ok;
        true ->
            ?CONSOLE("Starting eprof profiling\n", []),
            {ok, _Pid} = eprof:start(),
            profiling = eprof:start_profiling([self()])
    end,

    Workers = worker_specs(basho_bench_config:get(concurrent), []),
    {ok, {{one_for_one, 5, 10}, Workers}}.

%% ===================================================================
%% Internal functions
%% ===================================================================

worker_specs(0, Acc) ->
    Acc;
worker_specs(Count, Acc) ->
    Id = list_to_atom(lists:concat(['basho_bench_worker_', Count])),
    Spec = {Id, {basho_bench_worker, start_link, [Id, Count]},
            transient, 5000, worker, [basho_bench_worker]},
    worker_specs(Count-1, [Spec | Acc]).

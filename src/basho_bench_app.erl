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
-module(basho_bench_app).

-behaviour(application).

%% API
-export([start/0,
         stop/0,
         is_running/0]).

%% Application callbacks
-export([start/2, stop/1]).


%% ===================================================================
%% API
%%===================================================================

start() ->
    %% Redirect all SASL logging into a text file
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),
    ok = application:start(sasl),

    %% Make sure crypto is available
    ok = application:start(crypto),

    %% Start up our application -- mark it as permanent so that the node
    %% will be killed if we go down
    application:start(basho_bench, permanent).

stop() ->
    application:stop(basho_bench).

is_running() ->
    application:get_env(basho_bench_app, is_running) == {ok, true}.


%% ===================================================================
%% Application callbacks
%%===================================================================

start(_StartType, _StartArgs) ->
    {ok, Pid} = basho_bench_sup:start_link(),
    application:set_env(basho_bench_app, is_running, true),
    ok = basho_bench_stats:run(),
    ok = basho_bench_measurement:run(),
    ok = basho_bench_worker:run(basho_bench_sup:workers()),
    {ok, Pid}.


stop(_State) ->
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

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
         is_running/0,
         stop_or_kill/0]).

%% Application callbacks
-export([start/2, stop/1]).


%% ===================================================================
%% API
%%===================================================================

start() ->
    %% Redirect all SASL logging into a text file
    case application:get_env(basho_bench,app_run_mode) of
       {ok, included} ->
          %%Make sure sasl and crypto is available
          true=lists:keymember(sasl,1,application:which_applications()),
          true=lists:keymember(crypto,1,application:which_applications()),
          
          %% Start up our application
          application:start(basho_bench);
       NotInc when NotInc == {ok, standalone} orelse NotInc == undefined ->
          application:load(sasl),
          application:set_env(sasl, sasl_error_logger, {file, "log.sasl.txt"}),
          %% Make sure crypto is available
          ensure_started([sasl, crypto]),

          %% Start up our application -- mark it as permanent so that the node
          %% will be killed if we go down
          application:start(basho_bench, permanent)
    end.

stop() ->
    application:stop(basho_bench).

is_running() ->
    application:get_env(basho_bench_app, is_running) == {ok, true}.

stop_or_kill() ->
    %% If running standalone, halt and kill node.  Otherwise, just
    %% kill top supervisor.
    case application:get_env(basho_bench,app_run_mode) of
        {ok, included} ->
            exit(whereis(basho_bench_sup),kill);
        _ ->
            init:stop(1)
    end.

%% ===================================================================
%% Application callbacks
%%===================================================================

start(_StartType, _StartArgs) ->
    %% TODO: Move into a proper supervision tree, janky for now
    basho_bench_config:start_link(),
    {ok, Pid} = basho_bench_sup:start_link(),
    application:set_env(basho_bench_app, is_running, true),
    ok = basho_bench_stats:run(),
    ok = basho_bench_measurement:run(),
    ok = basho_bench_worker:run(basho_bench_sup:workers()),
    {ok, Pid}.


stop(_State) ->
    %% intentionally left in to show where worker profiling start/stop calls go.
    %% eprof:stop_profiling(),
    %% eprof:analyze(total),
    %% eprof:log("bb.eprof"),
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

ensure_started(Applications) when is_list(Applications) ->
  [ensure_started(Application) || Application <- Applications];

ensure_started(Application) ->
  case application:start(Application) of
    ok ->
      ok;
    {error, {already_started, Application}} ->
      ok;
    Error ->
      throw(Error)
  end.

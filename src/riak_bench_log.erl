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
-module(riak_bench_log).

-export([init/0,
         log/3]).

%% ===================================================================
%% Public API
%% ===================================================================

init() ->
    application:set_env(riak_bench, log_level, debug).

log(Level, Str, Args) ->
    {ok, LogLevel} = application:get_env(riak_bench, log_level),
    case should_log(LogLevel, Level) of
        true ->
            io:format(log_prefix(Level) ++ Str, Args);
        false ->
            ok
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

should_log(debug, _)     -> true;
should_log(info, debug)  -> false;
should_log(info, _)      -> true;
should_log(warn, debug)  -> false;
should_log(warn, info)   -> false;
should_log(warn, _)      -> true;
should_log(error, error) -> true;
should_log(error, _)     -> false;
should_log(_, _)         -> false.
    
log_prefix(debug) -> "DEBUG:" ;
log_prefix(info)  -> "INFO: ";
log_prefix(warn)  -> "WARN: ";
log_prefix(error) -> "ERROR: ".

     
    

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
-module(basho_bench_log).

-behaviour(gen_server).

%% API
-export([start_link/0,
         log/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { log_level,
                 log_file }).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

log(Level, Str, Args) ->
    case whereis(?MODULE) of
        undefined ->
            basic_log(Level, Str, Args);
        Pid ->
            gen_server:call(Pid, {log, Level, Str, Args})
    end.


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    LogLevel = basho_bench_config:get(log_level),
    {ok, LogFile} = file:open("log.txt", [raw, binary, write]),
    {ok, #state{ log_level = LogLevel,
                 log_file = LogFile }}.

handle_call({log, Level, Str, Args}, _From, State) ->
    case should_log(State#state.log_level, Level) of
        true ->
            Message = io_lib:format(log_prefix(Level) ++ Str, Args),
            ok = file:write(State#state.log_file, Message),
            ok = io:format(Message);
        false ->
            ok
    end,
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

basic_log(Level, Str, Args) ->
    {ok, LogLevel} = application:get_env(basho_bench, log_level),
    case should_log(LogLevel, Level) of
        true ->
            io:format(log_prefix(Level) ++ Str, Args);
        false ->
            ok
    end.

should_log(_, console)   -> true; 
should_log(debug, _)     -> true;
should_log(info, debug)  -> false;
should_log(info, _)      -> true;
should_log(warn, debug)  -> false;
should_log(warn, info)   -> false;
should_log(warn, _)      -> true;
should_log(error, error) -> true;
should_log(error, _)     -> false;
should_log(_, _)         -> false.

log_prefix(console) -> "";
log_prefix(debug)   -> "DEBUG:" ;
log_prefix(info)    -> "INFO: ";
log_prefix(warn)    -> "WARN: ";
log_prefix(error)   -> "ERROR: ".

     
    

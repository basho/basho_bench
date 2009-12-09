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
-module(riak_bench_stats).

-behaviour(gen_server).

%% API
-export([start_link/0,
         op_complete/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

op_complete(Op, Result, ElapsedUs) ->
    gen_server:cast(?MODULE, {op, Op, Result, ElapsedUs}).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({op, Op, ok, ElapsedUs}, State) ->
    {noreply, State};
handle_cast({op, Op, {error, Reason}, ElapsedUs}, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



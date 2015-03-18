%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
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
-module(basho_bench_driver_lager).

-export([new/1,
         run/4]).

-export([keygen/1, valgen/1, sink_generator/0, lager_msg_generator/0]).

-include("basho_bench.hrl").

-record(state, {
          multiple_sink_support = false
    }).

%% ====================================================================
%% API
%% ====================================================================

new(_ID) ->
    {ok, #state{multiple_sink_support = erlang:function_exported(lager, log, 5)}}.

run(log, SinkGen, ValueGen, State = #state{multiple_sink_support = S}) ->
    Sink = SinkGen(),
    {Level, Metadata, Format, Args} = ValueGen(),
    Result = case S of
        true -> 
            lager:log(Sink, Level, Metadata, Format, Args);
        false ->
            lager:log(Level, Metadata, Format, Args)
    end,
    case Result of 
        ok -> {ok, State};
        {error, lager_not_running} -> {'EXIT', lager_not_running};
        {error, Reason} -> {error, Reason, State}
    end.

keygen(_Id) ->
    fun sink_generator/0.

valgen(_Id) ->
    fun lager_msg_generator/0.

%% XXX FIXME: obviously a placeholder
sink_generator() ->
    ok.

%% XXX FIXME: obviously a placeholder
lager_msg_generator() ->
    {info, [], "~p", [<<"foo">>]}.

%% -------------------------------------------------------------------
%%
%% basho_bench: benchmark service on any within Erlang cluster 
%%              using distribution protocol
%%
%% Copyright (c) 2015 Dmitry Kolesnikov
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
-module(basho_bench_driver_cluster).

-export([
   new/1,
   run/4
]).

-record(state, {actor}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    Actors = basho_bench_config:get(cluster_actors, []),
    Nth    = (Id - 1) rem length(Actors) + 1,
    {Name, Node} = Actor = lists:nth(Nth, Actors),
    case net_adm:ping(Node) of
        pang ->
            lager:error("~s is not available", [Node]),
            {ok, #state{actor = undefined}};

        pong ->
            lager:info("worker ~b is bound to ~s on ~s", [Id, Name, Node]),
            {ok, #state{actor = Actor}}
    end.

run(Run, KeyGen, ValGen, #state{actor = Actor}=State) ->
    Key = KeyGen(),
    Val = ValGen(),
    erlang:send(Actor, {self(), Run, Key, Val}),
    receive
        {ok, ElapsedT} ->
            {ok, ElapsedT, State};

        {error, Reason} ->
            {error, Reason}
    end.

%% -------------------------------------------------------------------
%%
%% basho_bench_measurement_erlangvm: Measurement Driver for Erlang VMs.
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
-module(basho_bench_measurement_erlangvm).

-export([new/0,
         run/2]).

-include("basho_bench.hrl").

-record(state, {
          nodes
         }).

%% ====================================================================
%% API
%% ====================================================================

new() ->
    %% Try to spin up net_kernel
    MyNode  = basho_bench_config:get(mynode, [basho_bench, longnames]),
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    Nodes = basho_bench_config:get(nodes),
    Cookie = basho_bench_config:get(cookie),

    %% Initialize cookie for each of the nodes
    [true = erlang:set_cookie(N, Cookie) || N <- Nodes],

    %% Try to ping each of the nodes
    ping_each(Nodes),

    {ok, #state{ nodes = Nodes }}.

run(memory, State) ->
    %% Memory used: erlang:memory(total).
    F = fun(Node) ->
                rpc:call(Node, erlang, memory, [total])
        end,
    Memory = lists:sum([F(X) || X <- State#state.nodes]),
    {ok, Memory, State};

run(cpu, State) ->
    %% CPU load: cpu_sup:avg1() / 256.
    F = fun(Node) ->
                rpc:call(Node, cpu_sup, avg1, []) / 256 * 100
        end,
    AvgLoad = trunc(lists:sum([F(X) || X <- State#state.nodes]) / length(State#state.nodes)),
    {ok, AvgLoad, State};

run(processes, State) ->
    %% processes: length(erlang:processes()).
    F = fun(Node) ->
                %% We're sending back the process list, which sucks, but
                %% not sure if there is a better way to do it.
                length(rpc:call(Node, erlang, processes, []))
        end,
    Processes = lists:sum([F(X) || X <- State#state.nodes]),

    {ok, Processes, State};

run(filehandles, State) ->
    %% filehandles: list_to_integer(string:strip(string:strip(os:cmd("lsof -p" ++ os:getpid() ++ " | wc -l"), both), both, $\n)).
    F = fun(Node) ->
                %% We're sending back the process list, which sucks, but
                %% not sure if there is a better way to do it.
                Pid = rpc:call(Node, os, getpid, []),
                Cmd = io_lib:format("lsof -n -p ~p | wc -l", [Pid]),
                S1 = rpc:call(Node, os, cmd, [Cmd]),
                S2 = string:strip(S1, both),
                S3 = string:strip(S2, both, $\n),
                list_to_integer(S3)
        end,
    Filehandles = lists:sum([F(X) || X <- State#state.nodes]),
    {ok, Filehandles, State};

run(Measurement, _State) ->
    {unknown_measurement, Measurement}.

ping_each([]) ->
    ok;
ping_each([Node | Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            ping_each(Rest);
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.

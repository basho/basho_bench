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
-module(riak_bench_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { keygen,
                 valgen,
                 driver,
                 driver_state,
                 ops,
                 ops_len,
                 rng_seed,
                 worker_pid }).

-include("riak_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

start_link(Id) ->
    gen_server:start_link(?MODULE, [Id], []).

run(Pids) ->
    gen_server:multi_call(Pids, run).

stop(Pids) ->
    gen_server:multi_call(Pids, stop).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([Id]) ->
    %% Setup RNG seed for worker sub-process to use; incorporate the ID of
    %% the worker to ensure consistency in load-gen
    %%
    %% NOTE: If the worker process dies, this obviously introduces some entroy
    %% into the equation since you'd be restarting the RNG all over.
    {A1, A2, A3} = riak_bench_config:get(rng_seed),
    RngSeed = {A1+Id, A2+Id, A3+Id},
    
    %% Pull all config settings from environment
    Driver  = riak_bench_config:get(driver),
    Ops     = ops_tuple(),

    %% Spin up the driver implementation
    case catch(Driver:new()) of
        {ok, DriverState} ->
            ok;
        Error ->
            DriverState = undefined, % Make erlc happy
            ?FAIL_MSG("Failed to initialize driver ~p: ~p\n", [Driver, Error])
    end,

    %% Finally, initialize key and value generation. We pass in our ID to the initialization to
    %% enable (optional) key/value space partitioning
    KeyGen = riak_bench_keygen:new(riak_bench_config:get(key_generator), Id),
    ValGen = riak_bench_valgen:new(riak_bench_config:get(value_generator), Id),
    {ok, #state{ keygen = KeyGen, valgen = ValGen,
                 driver = Driver, driver_state = DriverState,
                 ops = Ops, ops_len = size(Ops),
                 rng_seed = RngSeed }}.

handle_call(run, _From, State) ->
    %% We use a dedicated sub-process to do the actual work. The work loop may need to sleep or
    %% otherwise delay in a way that would be inappropriate and/or inefficient for a
    %% gen_server. Furthermore, we want the loop to be as tight as possible for peak load
    %% generation and avoid unnecessary polling of the message queue.
    %%
    %% Link the worker and the sub-process to ensure that if either exits, the other goes with it.
    case State#state.worker_pid of
        undefined ->
            %% Setup stop event
            case riak_bench_config:get(timer:minutes(duration)) of
                infinite ->
                    ok;
                Duration ->
                    erlang:send_after(Duration, self(), stop)
            end,

            %% Kick off the worker
            Pid = spawn_link(fun() -> worker_init(State) end),
            {reply, ok, State#state { worker_pid = Pid }};
        _ ->
            {reply, {error, already_started}, State}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

%%
%% Expand operations list into tuple suitable for weighted, random draw
%%
ops_tuple() ->
    Ops = [lists:duplicate(Count, Op) || {Op, Count} <- riak_bench_config:get(operations)],
    list_to_tuple(lists:flatten(Ops)).
                                             

%%
%% Sub-process entry point that does the actual load gen
%%
worker_init(State) ->
    random:seed(State#state.rng_seed),
    worker_loop(State).

worker_loop(State) ->
    Next = element(random:uniform(State#state.ops_len), State#state.ops),
    Start = now(),
    Result = (catch (State#state.driver):run(Next, State#state.keygen, State#state.valgen,
                                             State#state.driver_state)),
    ElapsedUs = timer:now_diff(now(), Start),
    case Result of
        {ok, DriverState} ->
            %% Success
            riak_bench_stats:op_complete(Next, ok, ElapsedUs),
            worker_loop(State#state { driver_state = DriverState });

        {error, Reason, DriverState} ->
            %% Driver encountered a recoverable error
            riak_bench_stats:op_complete(Next, {error, Reason}, ElapsedUs),
            worker_loop(State#state { driver_state = DriverState });
        
        {'EXIT', Reason} ->
            %% Driver crashed, generate a crash error and terminate. This will take down
            %% the corresponding worker which will get restarted by the appropriate supervisor.
            ?ERROR("Driver ~p crashed: ~p\n", [Reason]),
            riak_bench_stats:op_complete(Next, {error, crash}, ElapsedUs)
    end.

    
    

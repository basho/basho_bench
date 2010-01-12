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
-export([start_link/1,
         run/1,
         stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { id,
                 keygen,
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
    [ok = gen_server:call(Pid, run) || Pid <- Pids],
    ok.

stop(Pids) ->
    [ok = gen_server:call(Pid, stop) || Pid <- Pids],
    ok.

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([Id]) ->
    %% Setup RNG seed for worker sub-process to use; incorporate the ID of
    %% the worker to ensure consistency in load-gen
    %%
    %% NOTE: If the worker process dies, this obviously introduces some entroy
    %% into the equation since you'd be restarting the RNG all over.
    process_flag(trap_exit, true),
    {A1, A2, A3} = riak_bench_config:get(rng_seed),
    RngSeed = {A1+Id, A2+Id, A3+Id},

    %% Pull all config settings from environment
    Driver  = riak_bench_config:get(driver),
    Ops     = ops_tuple(),

    %% Finally, initialize key and value generation. We pass in our ID to the
    %% initialization to enable (optional) key/value space partitioning
    KeyGen = riak_bench_keygen:new(riak_bench_config:get(key_generator), Id),
    ValGen = riak_bench_valgen:new(riak_bench_config:get(value_generator), Id),

    State = #state { id = Id, keygen = KeyGen, valgen = ValGen,
                     driver = Driver,
                     ops = Ops, ops_len = size(Ops),
                     rng_seed = RngSeed },

    %% Use a dedicated sub-process to do the actual work. The work loop may need
    %% to sleep or otherwise delay in a way that would be inappropriate and/or
    %% inefficient for a gen_server. Furthermore, we want the loop to be as
    %% tight as possible for peak load generation and avoid unnecessary polling
    %% of the message queue.
    %%
    %% Link the worker and the sub-process to ensure that if either exits, the
    %% other goes with it.
    WorkerPid = spawn_link(fun() -> worker_init(State) end),
    WorkerPid ! {init_driver, self()},
    receive
        driver_ready ->
            ok
    end,

    %% If the system is marked as running this is a restart; queue up the run
    %% message for this worker
    case riak_bench_app:is_running() of
        true ->
            ?WARN("Restarting crashed worker.\n", []),
            gen_server:cast(self(), run);
        false ->
            ok
    end,

    {ok, State#state { worker_pid = WorkerPid }}.

handle_call(run, _From, State) ->
    State#state.worker_pid ! run,
    {reply, ok, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(run, State) ->
    State#state.worker_pid ! run,
    {noreply, State}.

handle_info({'EXIT', _Pid, _Reason}, State) ->
    {stop, normal, State}.

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


worker_init(State) ->
    random:seed(State#state.rng_seed),
    worker_idle_loop(State).

worker_idle_loop(State) ->
    Driver = State#state.driver,
    receive
        {init_driver, Caller} ->
            %% Spin up the driver implementation
            case catch(Driver:new(State#state.id)) of
                {ok, DriverState} ->
                    Caller ! driver_ready,
                    ok;
                Error ->
                    DriverState = undefined, % Make erlc happy
                    ?FAIL_MSG("Failed to initialize driver ~p: ~p\n", [Driver, Error])
            end,
            worker_idle_loop(State#state { driver_state = DriverState });
        run ->
            case riak_bench_config:get(mode) of
                max ->
                    io:format("Max Worker runloop starting!\n"),
                    max_worker_run_loop(State);
                {rate, Rate} ->
                    %% Calculate mean interarrival time in in milliseconds. A
                    %% fixed rate worker can generate (at max) only 1k req/sec.
                    MeanArrival = 1000 / Rate,
                    io:format("Fixed Rate Worker runloop starting: ~w ms/req\n", [MeanArrival]),
                    rate_worker_run_loop(State, 1 / MeanArrival)
            end
    end.

worker_next_op(State) ->
    Next = element(random:uniform(State#state.ops_len), State#state.ops),
    Start = now(),
    Result = (catch (State#state.driver):run(Next, State#state.keygen, State#state.valgen,
                                             State#state.driver_state)),
    ElapsedUs = timer:now_diff(now(), Start),
    case Result of
        {ok, DriverState} ->
            %% Success
            riak_bench_stats:op_complete(Next, ok, ElapsedUs),
            {ok, State#state { driver_state = DriverState}};

        {error, Reason, DriverState} ->
            %% Driver encountered a recoverable error
            riak_bench_stats:op_complete(Next, {error, Reason}, ElapsedUs),
            {ok, State#state { driver_state = DriverState}};

        {'EXIT', Reason} ->
            %% Driver crashed, generate a crash error and terminate. This will take down
            %% the corresponding worker which will get restarted by the appropriate supervisor.
            riak_bench_stats:op_complete(Next, {error, crash}, ElapsedUs),
            ?ERROR("Driver ~p crashed: ~p\n", [State#state.driver, Reason]),
            error
    end.

max_worker_run_loop(State) ->
    case worker_next_op(State) of
        {ok, State2} ->
            max_worker_run_loop(State2);
        error ->
            error
    end.

rate_worker_run_loop(State, Lambda) ->
    %% Delay between runs using exponentially distributed delays to mimic
    %% queue.
    timer:sleep(trunc(stats_rv:exponential(Lambda))),
    case worker_next_op(State) of
        {ok, State2} ->
            rate_worker_run_loop(State2, Lambda);
        error ->
            error
    end.

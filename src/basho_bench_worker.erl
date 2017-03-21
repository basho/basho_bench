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
-module(basho_bench_worker).

-behaviour(gen_server).

%% API
-export([start_link/3,
         start_link_local/3,
         run/1,
         stop/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% State accessors
-export([get_keygen/2, get_valgen/2]).

-record(state, { id,
                 keygen,
                 valgen,
                 driver,
                 driver_state,
                 api_pass_state,
                 worker_type,
                 local_config,
                 shutdown_on_error,
                 should_report_stats,
                 ops,
                 ops_len,
                 rng_seed,
                 parent_pid,
                 worker_pid,
                 sup_id}).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

start_link(SupChild, Id, WorkerConf) ->
    case basho_bench_config:get(distribute_work, false) of
        true ->
            start_link_distributed(SupChild, Id, WorkerConf);
        false ->
            start_link_local(SupChild, Id, WorkerConf)
    end.

start_link_distributed(SupChild, Id, WorkerConf) ->
    Node = pool:get_node(),
    rpc:block_call(Node, ?MODULE, start_link_local, [SupChild, Id, WorkerConf]).

start_link_local(SupChild, Id, WorkerConf) ->
    gen_server:start_link(?MODULE, [SupChild, Id, WorkerConf], []).

run(Pids) ->
    [ok = gen_server:call(Pid, run, infinity) || Pid <- Pids],
    ok.

stop(Pids) ->
    [ok = gen_server:call(Pid, stop, infinity) || Pid <- Pids],
    ok.

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([SupChild, {WorkerType, WorkerId, WorkerGlobalId}=Id, WorkerConf]) ->
    %% Set local worker config here and for subprocess during worker_init
    basho_bench_config:set_local_config(WorkerConf),

    %% Setup RNG seed for worker sub-process to use; incorporate the ID of
    %% the worker to ensure consistency in load-gen
    %%
    %% NOTE: If the worker process dies, this obviously introduces some entroy
    %% into the equation since you'd be restarting the RNG all over.
    %%
    %% The RNG_SEED is static by default for replicability of key size
    %% and value size generation between test runs.
    process_flag(trap_exit, true),
    {A1, A2, A3} =
        case basho_bench_config:get(rng_seed, {42, 23, 12}) of
            {Aa, Ab, Ac} -> {Aa, Ab, Ac};
            now -> now()
        end,

    RngSeed = {A1 + WorkerGlobalId, A2 + WorkerGlobalId, A3 + WorkerGlobalId},

    %% Pull all config settings from environment
    Driver = basho_bench_config:get(driver),
    Operations = basho_bench_config:get(operations),
    Ops = ops_tuple(Operations),
    ShutdownOnError = basho_bench_config:get(shutdown_on_error, false),

    %% Check configuration for flag enabling new API that passes opaque State object to support accessor functions
    State0 = #state { id = Id,
                     api_pass_state = basho_bench_config:get(api_pass_state),
                     driver = Driver,
                     local_config = WorkerConf,
                     worker_type = WorkerType,
                     shutdown_on_error = ShutdownOnError,
                     should_report_stats = basho_bench_config:get(should_report_stats, true),
                     ops = Ops,
                     ops_len = size(Ops),
                     rng_seed = RngSeed,
                     parent_pid = self(),
                     sup_id = SupChild
                     },

    %% Finally, initialize key and value generation. We pass in our ID to the
    %% initialization to enable (optional) key/value space partitioning
    State = add_generators(State0),

    %% Use a dedicated sub-process to do the actual work. The work loop may need
    %% to sleep or otherwise delay in a way that would be inappropriate and/or
    %% inefficient for a gen_server. Furthermore, we want the loop to be as
    %% tight as possible for peak load generation and avoid unnecessary polling
    %% of the message queue.
    %%
    %% Link the worker and the sub-process to ensure that if either exits, the
    %% other goes with it.
    %% WorkerPid is the driver process id which is linked with basho_bench_worker id
    WorkerPid = spawn_link(fun() -> worker_init(State) end),
    WorkerPid ! {init_driver, self()},
    receive
        driver_ready ->
            ok;
        {init_driver_failed, Why} ->
            exit({init_driver_failed, Why})
    end,

    %% If the system is marked as running this is a restart; queue up the run
    %% message for this worker
    case basho_bench_app:is_running() of
        true ->
            ?WARN("Restarting crashed worker.\n", []),
            gen_server:cast(self(), run);
        false ->
            ok
    end,
    {ok, State#state { worker_pid = WorkerPid }}.

handle_call(run, _From, State) ->
    State#state.worker_pid ! run,
    {reply, ok, State}.

handle_cast(run, State) ->
    State#state.worker_pid ! run,
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    #state{worker_pid=WorkerPid} = State,
    case {Reason, Pid} of
        {normal, _} ->
            %% Worker process exited normally
            %% Stop this worker and check is there any other alive worker
            basho_bench_duration:worker_stopping(self()),
            {stop, normal, State};
        {_, WorkerPid} ->
            ?ERROR("Worker ~p exited with ~p~n", [Pid, Reason]),
            %% Worker process exited for some other reason; stop this process
            %% as well so that everything gets restarted by the sup
            {stop, worker_died, State}
    end.

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
ops_tuple(Operations) ->
    F =
        fun({OpTag, Count}) ->
                lists:duplicate(Count, {OpTag, OpTag});
           ({Label, OpTag, Count}) ->
                lists:duplicate(Count, {Label, OpTag});
           ({Label, OpTag, Count, _OptionsList}) ->
                lists:duplicate(Count, {Label, OpTag})
        end,
    Ops = [F(X) || X <- Operations],
    list_to_tuple(lists:flatten(Ops)).

worker_init(State) ->
    %% Trap exits from linked parent process; use this to ensure the driver
    %% gets a chance to cleanup
    process_flag(trap_exit, true),
    %% Publish local config into worker subprocess
    basho_bench_config:set_local_config(State#state.local_config),
    random:seed(State#state.rng_seed),
    worker_idle_loop(State).

worker_idle_loop(State) ->
    Driver = State#state.driver,
    receive
        {init_driver, Caller} ->
            %% Spin up the driver implementation, optionally support new approach of passing State
            DriverNew = case State#state.api_pass_state of
                    false -> catch(Driver:new(State#state.id));
                     _ -> catch(Driver:new(State#state.id, State))
                end,
            case DriverNew of
                {ok, DriverState} ->
                    Caller ! driver_ready,
                    ok;
                Error ->
                    ?FAIL_MSG("Failed to initialize driver ~p: ~p\n", [Driver, Error]),
                    DriverState = undefined, % Make erlc happy
                    Caller ! {init_driver_failed, Error}
            end,
            worker_idle_loop(State#state { driver_state = DriverState });
        run ->
            case basho_bench_config:get(mode) of
                max ->
                    ?INFO("Starting max worker: ~p on ~p~n", [self(), node()]),
                    max_worker_run_loop(State);
                {rate, max} ->
                    ?INFO("Starting max worker: ~p on ~p~n", [self(), node()]),
                    max_worker_run_loop(State);
                {rate, Rate} ->
                    %% Calculate mean interarrival time in in milliseconds. A
                    %% fixed rate worker can generate (at max) only 1k req/sec.
                    MeanArrival = 1000 / Rate,
                    ?INFO("Starting ~w ms/req fixed rate worker: ~p on ~p\n", [MeanArrival, self(), node()]),
                    rate_worker_run_loop(State, 1 / MeanArrival)
            end
    end.

%% Traditional call to run/4 passing OpTag, keygen, valgen, and driver state
worker_next_op2(#state{api_pass_state=false}=State, OpTag) ->
    catch (State#state.driver):run(OpTag, State#state.keygen, State#state.valgen,State#state.driver_state);
%% When using api_pass_state, call run/3 passing Optag, driver state and worker state,
%% then use accessors to get_keygen or get_valgen using State
worker_next_op2(State, OpTag) ->
    catch (State#state.driver):run(OpTag, State#state.driver_state, State).

worker_next_op(State) ->
    {Label, OpTag} = element(random:uniform(State#state.ops_len), State#state.ops),
    Start = os:timestamp(),
    Result = worker_next_op2(State, OpTag),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),

    OpName = { basho_bench_stats:worker_op_name(State#state.worker_type, Label),
               basho_bench_stats:worker_op_name(State#state.worker_type, OpTag)},
    case Result of
        {Res, DriverState} when Res == ok orelse element(1, Res) == ok ->
            maybe_report_stats(OpName, Res, ElapsedUs, State),
            {ok, State#state { driver_state = DriverState}};

        {Res, DriverState} when Res == silent orelse element(1, Res) == silent ->
            {ok, State#state { driver_state = DriverState}};

        {ok, ElapsedT, DriverState} ->
            %% time is measured by external system
            maybe_report_stats(OpName, ok, ElapsedT, State),
            {ok, State#state { driver_state = DriverState}};

        {error, Reason, DriverState} ->
            %% Driver encountered a recoverable error
            maybe_report_stats(OpName, {error, Reason}, ElapsedUs, State),
            State#state.shutdown_on_error andalso
                erlang:send_after(500, basho_bench,
                                  {shutdown, "Shutdown on errors requested", 1}),
            {ok, State#state { driver_state = DriverState}};

        {'EXIT', Reason} ->
            %% Driver crashed, generate a crash error and terminate. This will take down
            %% the corresponding worker which will get restarted by the appropriate supervisor.
            maybe_report_stats(OpName, {error, crash}, ElapsedUs, State),

            %% Give the driver a chance to cleanup
            (catch (State#state.driver):terminate({'EXIT', Reason}, State#state.driver_state)),

            ?DEBUG("Driver ~p crashed: ~p\n", [State#state.driver, Reason]),
            case State#state.shutdown_on_error of
                true ->
                    %% Yes, I know this is weird, but currently this
                    %% is how you tell Basho Bench to return a
                    %% non-zero exit status.  Ideally this would all
                    %% be done in the `handle_info' callback where it
                    %% would check `Reason' and `shutdown_on_error'.
                    %% Then I wouldn't have to return a bullshit "ok"
                    %% here.
                    erlang:send_after(500, basho_bench,
                                      {shutdown, "Shutdown on errors requested", 2}),
                    {ok, State};
                false ->
                    crash
            end;

        {stop, Reason} ->
            %% Driver (or something within it) has requested that this worker
            %% terminate cleanly.
            ?INFO("Driver ~p (~p) has requested stop: ~p\n", [State#state.driver, self(), Reason]),

            %% Give the driver a chance to cleanup
            (catch (State#state.driver):terminate(normal, State#state.driver_state)),

            normal
    end.

needs_shutdown(State) ->
    receive
        {'EXIT', _Pid, _Reason} ->
            (catch (State#state.driver):terminate(normal,
                                                  State#state.driver_state)),
            true
    after 0 ->
            false
    end.


max_worker_run_loop(State) ->
    case worker_next_op(State) of
        {ok, State2} ->
            case needs_shutdown(State2) of
                true ->
                    ok;
                false ->
                    max_worker_run_loop(State2)
            end;
        ExitReason ->
            exit(ExitReason)
    end.

rate_worker_run_loop(State, Lambda) ->
    %% Delay between runs using exponentially distributed delays to mimic
    %% queue.
    timer:sleep(trunc(basho_bench_stats:exponential(Lambda))),
    case worker_next_op(State) of
        {ok, State2} ->
            case needs_shutdown(State2) of
                true ->
                    ok;
                false ->
                    rate_worker_run_loop(State2, Lambda)
            end;
        ExitReason ->
            exit(ExitReason)
    end.

maybe_report_stats(Op, Result, Stat, State) ->
    case State#state.should_report_stats of
        true ->
            basho_bench_stats:op_complete(Op, Result, Stat);
        false ->
            ok
    end.

add_generators(#state{api_pass_state=ApiPassState, id=Id}=State) ->
    {_WorkerType, WorkerId, _WorkerGlobalId} = Id,
    % KeyGen needs to know WorkerId within a WorkerType (local concurrent) number of workers sharing a thread
    KeyGen = init_generators(ApiPassState, basho_bench_config:get(key_generator), WorkerId, basho_bench_keygen),
    ValGen = init_generators(ApiPassState, basho_bench_config:get(value_generator), WorkerId, basho_bench_valgen),
    State#state{keygen=KeyGen, valgen=ValGen}.

%% Not passing state API - expect non-list spec or error during new attempt
init_generators(false, Config, Id, Module) when is_tuple(Config) ->
    Module:new(Config, Id);
%% Passing state API - process list or turn single-spec into [{default,Generator}]
init_generators(true, Configs, Id, Module) when is_list(Configs) ->
    [{N, Module:new(S, Id)} || {N,S} <- Configs].

%% Accessor functions

get_keygen(Name, State) ->
    proplists:get_value(Name, State#state.keygen).

get_valgen(Name, State) ->
    proplists:get_value(Name, State#state.valgen).


%% -------------------------------------------------------------------
%% Check whether to exit besho_bench, exit conditions
%%     1. run out of duration time
%%     2. run out of operations even if the duration time had not up
%% -------------------------------------------------------------------

-module(basho_bench_duration).

-behavior(gen_server).

-export([
    run/0,
    remaining/0
]).

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    worker_stopping/1
]).

-record(state, {
    ref,
    duration,
    start
}).

-include("basho_bench.hrl").


run() ->
    Timeout = basho_bench_config:get(duration_call_run_timeout),
    gen_server:call(?MODULE, run, Timeout).


remaining() ->
    gen_server:call(?MODULE, remaining).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
    run_hook(basho_bench_config:get(pre_hook, no_op)),
    Ref = erlang:monitor(process, whereis(basho_bench_run_sup)),
    {ok, #state{ref=Ref}}.


handle_call(run, _From, State) ->
    DurationMins = basho_bench_config:get(duration, 1),
    ?INFO("Starting with duration: ~p", [DurationMins]),
    NewState = State#state{
        start=os:timestamp(),
        duration=DurationMins
    },
    maybe_end({reply, ok, NewState});

handle_call(remaining, _From, State) ->
    #state{start=Start, duration=Duration} = State,
    Remaining = (Duration*60) - timer:now_diff(os:timestamp(), Start),
    maybe_end({reply, Remaining, State}).


%% WorkerPid is basho_bench_worker's id, not the pid of actual driver
handle_cast({worker_stopping, WorkerPid}, State) ->
    case basho_bench_worker_sup:active_workers() -- [WorkerPid] of
        [] ->
            ?INFO("The application has stopped early!", []), 
            {stop, {shutdown, normal}, State}; 
        _ -> 
            maybe_end({noreply, State}) 
    end;

handle_cast(_Msg, State) ->
    maybe_end({noreply, State}).


handle_info({'DOWN', Ref, process, _Object, Info}, #state{ref=Ref}=State) ->
    {stop, {shutdown, Info}, State};

handle_info(timeout, State) ->
    {stop, {shutdown, normal}, State}.


terminate(Reason, #state{duration=DurationMins}) ->
    case whereis(basho_bench_worker_sup) of
        undefined ->
            ok;
        WSup ->
            WRef = erlang:monitor(process, WSup),
            supervisor:terminate_child(basho_bench_run_sup, basho_bench_worker_sup),
            receive
                {'DOWN', WRef, process, _Object, _Info} ->
                    ok
            end
    end,
    run_hook(basho_bench_config:get(post_hook, no_op)),
    case basho_bench_config:get(enable_eprof, false) of 
        false ->
            ok;
        true ->
            ?CONSOLE("Stopping eprof profiling", []),
            EprofFile = filename:join(basho_bench:get_test_dir(), "eprof.log"),
            eprof:stop_profiling(),
            eprof:log(EprofFile),
            eprof:analyze(total),
            ?CONSOLE("Eprof output in ~p\n", [EprofFile])
    end,
    supervisor:terminate_child(basho_bench_sup, basho_bench_run_sup),
    case Reason of
        {shutdown, normal} ->
            ?CONSOLE("Test completed after ~p mins.\n", [DurationMins]);
        {shutdown, Reason} ->
            ?CONSOLE("Test stopped: ~p\n", [Reason])
    end,
    application:set_env(basho_bench_app, is_running, false),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


maybe_end(Return) ->
    {Reply, Message, State} = case Return of
        {Reply0, Message0, State0} ->
            {Reply0, Message0, State0};
        {Reply0, State0} ->
            {Reply0, ok, State0}
    end,
    #state{start=Start} = State,
    case State#state.duration of
        infinity ->
            Return;
        Duration ->
            case timer:now_diff(os:timestamp(), Start) of
                Elapsed when Elapsed / 60000000 >= Duration ->
                    ?CONSOLE("Stopping: ~p", [Elapsed]),
                    {stop, normal, State};
                Elapsed ->
                    Timeout = round(Duration*60000 - Elapsed/1000),
                    case tuple_size(Return) of
                        2 -> {Reply, State, Timeout};
                        3 -> {Reply, Message, State, Timeout}
                    end
            end
    end.


run_hook({Module, Function}) ->
    Module:Function();

run_hook(no_op) ->
    no_op.

worker_stopping(WorkerPid) ->
    %% WorkerPid is basho_bench_worker's id, not the pid of actual driver 
    gen_server:cast(?MODULE, {worker_stopping, WorkerPid}),
    ok.

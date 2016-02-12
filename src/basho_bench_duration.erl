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
    code_change/3
]).

-record(state, {
    ref,
    duration,
    start
}).

-include("basho_bench.hrl").


run() ->
    gen_server:call(?MODULE, run).


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
    lager:info("Starting with duration: ~p", [DurationMins]),
    NewState = State#state{
        start=os:timestamp(),
        duration=DurationMins
    },
    maybe_end({reply, ok, NewState});

handle_call(remaining, _From, State) ->
    #state{start=Start, duration=Duration} = State,
    Remaining = (Duration*60) - timer:now_diff(os:timestamp(), Start),
    maybe_end({reply, Remaining, State}).


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

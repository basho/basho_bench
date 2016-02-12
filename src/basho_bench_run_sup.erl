-module(basho_bench_run_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, transient, 30000, Type, [I]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    MeasurementDriver = case basho_bench_config:get(measurement_driver, []) of
        [] -> [];
        _Driver -> [?CHILD(basho_bench_measurement, worker)]
    end,
    Spec = [
        ?CHILD(basho_bench_duration, worker),
        ?CHILD(basho_bench_stats, worker),
        ?CHILD(basho_bench_worker_sup, supervisor)
    ] ++ MeasurementDriver,
    {ok, {{one_for_all, 0, 1}, Spec}}.

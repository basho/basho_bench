-module(basho_bench_profiler).

-export([ 
    maybe_start_profiler/1, 
    maybe_start_profiler/2, 
    maybe_terminate_profiler/1, 
    maybe_terminate_profiler/2
]).

-include("basho_bench.hrl").

maybe_start_profiler(Profiler) ->
    maybe_start_profiler(Profiler, basho_bench:get_test_dir()).

maybe_start_profiler(false, _Dir) ->
    {ok, 0};
maybe_start_profiler(cprof, _Dir) ->
    ?CONSOLE("Starting cprof profiling\n", []),
    FuncCount = cprof:start(),
    {ok, FuncCount};
maybe_start_profiler(eprof, _Dir) ->
    ?CONSOLE("Starting eprof profiling\n", []),
    {ok, Pid} = eprof:start(),
    profiling = eprof:start_profiling([self()]),
    {ok, Pid};
maybe_start_profiler(fprof, Dir) ->
    FprofTraceFile = filename:join(Dir, "fprofTrace.log"),
    ?CONSOLE("Starting fprof profiling to ~p\n", [FprofTraceFile]),
    {ok, Pid} = fprof:start(),
    fprof:trace([start, {file, FprofTraceFile}]),
    {ok, Pid}.

maybe_terminate_profiler(Profiler) ->
    maybe_terminate_profiler(Profiler,  basho_bench:get_test_dir()).

maybe_terminate_profiler(false, _Dir) ->
    ok;
maybe_terminate_profiler(cprof, Dir) ->
    CprofFile = filename:join(Dir, "cprof.log"),
    ?CONSOLE("Writing cprof profiling to ~p\n", [CprofFile]),
    CprofData = cprof:analyse(),
    file:write_file(CprofFile, io_lib:fwrite("~p.\n", [CprofData])),
    cprof:stop();
maybe_terminate_profiler(eprof, Dir) ->
    ?CONSOLE("Stopping eprof profiling", []),
    EprofFile = filename:join(Dir, "eprof.log"),
    eprof:stop_profiling(),
    eprof:log(EprofFile),
    eprof:analyze(total),
    ?CONSOLE("Eprof output in ~p\n", [EprofFile]);
maybe_terminate_profiler(eprof, Dir) ->
    FprofTraceFile = filename:join(Dir, "fprofTrace.log"),
    FprofFile = filename:join(Dir, "fprof.log"),
    ?CONSOLE("Stopping fprof profiling, writing to ~p\n", [FprofFile]),
    fprof:trace(stop),
    fprof:profile({file, FprofTraceFile}),
    fprof:analyse([{dest, FprofFile}]),
    file:delete(FprofTraceFile).

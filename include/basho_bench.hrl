

-define(FAIL_MSG(Str, Args), ?ERROR(Str, Args), halt(1)).

-define(CONSOLE(Str, Args), basho_bench_log:log(console, Str, Args)).

-define(DEBUG(Str, Args), basho_bench_log:log(debug, Str, Args)).
-define(INFO(Str, Args), basho_bench_log:log(info, Str, Args)).
-define(WARN(Str, Args), basho_bench_log:log(warn, Str, Args)).
-define(ERROR(Str, Args), basho_bench_log:log(error, Str, Args)).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).


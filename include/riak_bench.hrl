

-define(FAIL_MSG(Str, Args), ?ERROR(Str, Args), halt(1)).

-define(CONSOLE(Str, Args), io:format(Str, Args)).

-define(DEBUG(Str, Args), riak_bench_log:log(debug, Str, Args)).
-define(INFO(Str, Args), riak_bench_log:log(info, Str, Args)).
-define(WARN(Str, Args), riak_bench_log:log(warn, Str, Args)).
-define(ERROR(Str, Args), riak_bench_log:log(error, Str, Args)).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).


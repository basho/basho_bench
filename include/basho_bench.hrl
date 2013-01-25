
-define(FAIL_MSG(Str, Args), ?ERROR(Str, Args), basho_bench_app:halt_or_kill()).
-define(STD_ERR(Str, Args), io:format(standard_error, Str, Args)).

-define(CONSOLE(Str, Args), lager:info(Str, Args)).

-define(DEBUG(Str, Args), lager:debug(Str, Args)).
-define(INFO(Str, Args), lager:info(Str, Args)).
-define(WARN(Str, Args), lager:warning(Str, Args)).
-define(ERROR(Str, Args), lager:error(Str, Args)).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).


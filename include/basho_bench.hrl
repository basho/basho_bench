
-define(FAIL_MSG(Str, Args), ?ERROR(Str, Args), basho_bench_app:stop_or_kill()).
-define(STD_ERR(Str, Args), io:format(standard_error, Str, Args)).

-define(CONSOLE(Str, Args), _ = lager:info(Str, Args)).

-define(DEBUG(Str, Args), _ = lager:debug(Str, Args)).
-define(INFO(Str, Args), _ = lager:info(Str, Args)).
-define(WARN(Str, Args), _ = lager:warning(Str, Args)).
-define(ERROR(Str, Args), _ = lager:error(Str, Args)).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

-define(VAL_GEN_BLOB_CFG, value_generator_blob_file).
-define(VAL_GEN_SRC_SIZE, value_generator_source_size).

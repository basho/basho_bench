
-define(FAIL_MSG(Str, Args), ?ERROR(Str, Args), basho_bench_app:stop_or_kill()).
-define(STD_ERR(Str, Args), io:format(standard_error, Str, Args)).

-ifdef(no_lager).

-define(CONSOLE(Str, Args), io:format(Str, Args)).

-define(DEBUG(Str, Args), error_logger:debug_msg(Str, Args)).
-define(INFO(Str, Args), error_logger:info_msg(Str, Args)).
-define(WARN(Str, Args), error_logger:warn_msg(Str, Args)).
-define(ERROR(Str, Args), error_logger:error_msg(Str, Args)).

-else.

-define(CONSOLE(Str, Args), lager:info(Str, Args)).

-define(DEBUG(Str, Args), lager:debug(Str, Args)).
-define(INFO(Str, Args), lager:info(Str, Args)).
-define(WARN(Str, Args), lager:warning(Str, Args)).
-define(ERROR(Str, Args), lager:error(Str, Args)).

-endif.

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

-define(VAL_GEN_BLOB_CFG, value_generator_blob_file).
-define(VAL_GEN_SRC_SIZE, value_generator_source_size).

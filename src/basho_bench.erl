%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2012 Basho Techonologies
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
-module(basho_bench).

-export([main/1, md5/1]).
-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

cli_options() ->
    [
     {help, $h, "help", undefined, "Print usage"},
     {results_dir, $d, "results-dir", string, "Base directory to store test results, defaults to ./tests"},
     {bench_name, $n, "bench-name", string, "Name to identify the run, defaults to timestamp"},
     {net_node,   $N, "node",   atom, "Erlang long node name of local node (initiating basho_bench)"},
     {net_cookie, $C, "cookie", {atom, benchmark}, "Erlang network distribution magic cookie"},
     {net_join,   $J, "join",   atom, "Erlang long node name of remote node (to join to)"}
    ].

main(Args) ->
    {Opts, Configs} = check_args(getopt:parse(cli_options(), Args)),
    ok = maybe_show_usage(Opts),
    ok = maybe_net_node(Opts),
    ok = maybe_join(Opts),
    BenchName = bench_name(Opts),
    TestDir = test_dir(Opts, BenchName),

    %% Load baseline configs
    case application:load(basho_bench) of
        ok -> ok;
        {error, {already_loaded, basho_bench}} -> ok
    end,
    register(basho_bench, self()),
    %% TODO: Move into a proper supervision tree, janky for now
    {ok, _Pid} = basho_bench_config:start_link(),
    basho_bench_config:set(test_id, BenchName),

    application:load(lager),
    ConsoleLagerLevel = basho_bench_config:get(log_level, debug),
    ErrorLog = filename:join([TestDir, "error.log"]),
    ConsoleLog = filename:join([TestDir, "console.log"]),
    CrashLog = filename:join([TestDir, "crash.log"]),
    application:set_env(lager,
                        handlers,
                        [{lager_console_backend, ConsoleLagerLevel},
                         {lager_file_backend, [{file, ErrorLog},   {level, error}, {size, 10485760}, {date, "$D0"}, {count, 5}]},
                         {lager_file_backend, [{file, ConsoleLog}, {level, debug}, {size, 10485760}, {date, "$D0"}, {count, 5}]}
                        ]),
    application:set_env(lager, crash_log, CrashLog),
    lager:start(),

    %% Make sure this happens after starting lager or failures wont
    %% show.
    basho_bench_config:load(Configs),

    %% Log level can be overriden by the config files
    CustomLagerLevel = basho_bench_config:get(log_level),
    lager:set_loglevel(lager_console_backend, CustomLagerLevel),
    lager:set_loglevel(lager_file_backend, ConsoleLog, CustomLagerLevel),

    %% Init code path
    add_code_paths(basho_bench_config:get(code_paths, [])),

    %% If a source directory is specified, compile and load all .erl files found
    %% there.
    case basho_bench_config:get(source_dir, []) of
        [] ->
            ok;
        SourceDir ->
            load_source_files(SourceDir)
    end,

    %% Copy the config into the test dir for posterity
    [ begin {ok, _} = file:copy(Config, filename:join(TestDir, filename:basename(Config))) end
      || Config <- Configs ],
    %% Set our CWD to the test dir
    ok = file:set_cwd(TestDir),
    log_dimensions(),

    %% Run pre_hook for user code preconditions
    run_pre_hook(),
    %% Spin up the application
    ok = basho_bench_app:start(),

    %% Pull the runtime duration from the config and sleep until that's passed OR
    %% the supervisor process exits
    Mref = erlang:monitor(process, whereis(basho_bench_sup)),
    DurationMins = basho_bench_config:get(duration),
    wait_for_stop(Mref, DurationMins).


%% ====================================================================
%% Internal functions
%% ====================================================================

print_usage() ->
    getopt:usage(cli_options(), escript:script_name(), "CONFIG_FILE").

check_args({ok, {Opts, Args}}) ->
    {Opts, Args};
check_args({error, {Reason, _Data}}) ->
    ?STD_ERR("Failed to parse arguments: ~p~n", [Reason]),
    print_usage(),
    halt(1).

maybe_show_usage(Opts) ->
    case lists:member(help, Opts) of
        true ->
            print_usage(),
            halt(0);
        false ->
            ok
    end.

maybe_net_node(Opts) ->
    case lists:keyfind(net_node, 1, Opts) of
        {_, Node} ->
            {_, Cookie} = lists:keyfind(net_cookie, 1, Opts),
            os:cmd("epmd -daemon"),
            net_kernel:start([Node, longnames]),
            erlang:set_cookie(Node, Cookie),
            ok;
        false ->
            ok
    end.

maybe_join(Opts) ->
    case lists:keyfind(net_join, 1, Opts) of
        {_, Host} ->
            case net_adm:ping(Host) of
                pong -> global:sync();
                _    -> throw({no_host, Host})
            end;
        false ->
            ok
    end.

bench_name(Opts) ->
    case proplists:get_value(bench_name, Opts, id()) of
        "current" ->
            ?STD_ERR("Cannot use name 'current'~n", []),
            halt(1);
        Name ->
            Name
    end.

test_dir(Opts, Name) ->
    {ok, CWD} = file:get_cwd(),
    DefaultResultsDir = filename:join([CWD, "tests"]),
    ResultsDir = proplists:get_value(results_dir, Opts, DefaultResultsDir),
    ResultsDirAbs = filename:absname(ResultsDir),
    TestDir = filename:join([ResultsDirAbs, Name]),
    {ok, TestDir} = {filelib:ensure_dir(filename:join(TestDir, "foobar")), TestDir},
    Link = filename:join([ResultsDir, "current"]),
    [] = os:cmd(?FMT("rm -f ~s; ln -sf ~s ~s", [Link, TestDir, Link])),
    TestDir.

wait_for_stop(Mref, infinity) ->
    receive
        {'DOWN', Mref, _, _, Info} ->
            run_post_hook(),
            ?CONSOLE("Test stopped: ~p\n", [Info])
    end;
wait_for_stop(Mref, DurationMins) ->
    Duration = timer:minutes(DurationMins) + timer:seconds(1),
    receive
        {'DOWN', Mref, _, _, Info} ->
            run_post_hook(),
            ?CONSOLE("Test stopped: ~p\n", [Info]);
        {shutdown, Reason, Exit} ->
            run_post_hook(),
            basho_bench_app:stop(),
            ?CONSOLE("Test shutdown: ~s~n", [Reason]),
            halt(Exit)

    after Duration ->
            run_post_hook(),
            basho_bench_app:stop(),
            ?CONSOLE("Test completed after ~p mins.\n", [DurationMins])
    end.

%%
%% Construct a string suitable for use as a unique ID for this test run
%%
id() ->
    {{Y, M, D}, {H, Min, S}} = calendar:local_time(),
    ?FMT("~w~2..0w~2..0w_~2..0w~2..0w~2..0w", [Y, M, D, H, Min, S]).

add_code_paths([]) ->
    ok;
add_code_paths([Path | Rest]) ->
    Absname = filename:absname(Path),
    CodePath = case filename:basename(Absname) of
                   "ebin" ->
                       Absname;
                   _ ->
                       filename:join(Absname, "ebin")
               end,
    case code:add_path(CodePath) of
        true ->
            add_code_paths(Rest);
        Error ->
            ?FAIL_MSG("Failed to add ~p to code_path: ~p\n", [CodePath, Error])
    end.


%%
%% Convert a number of bytes into a more user-friendly representation
%%
user_friendly_bytes(Size) ->
    lists:foldl(fun(Desc, {Sz, SzDesc}) ->
                        case Sz > 1000 of
                            true ->
                                {Sz / 1024, Desc};
                            false ->
                                {Sz, SzDesc}
                        end
                end,
                {Size, bytes}, ['KB', 'MB', 'GB']).

log_dimensions() ->
    case basho_bench_keygen:dimension(basho_bench_config:get(key_generator)) of
        undefined ->
            ok;
        Keyspace ->
            Valspace = basho_bench_valgen:dimension(basho_bench_config:get(value_generator), Keyspace),
            {Size, Desc} = user_friendly_bytes(Valspace),
            ?INFO("Est. data size: ~.2f ~s\n", [Size, Desc])
    end.


load_source_files(Dir) ->
    CompileFn = 
        fun(F, _Acc) ->
            case compile:file(F, [report, binary]) of
                {ok, Mod, Bin} ->
                    {module, Mod} = code:load_binary(Mod, F, Bin),
                    ?INFO("Loaded ~p (~s)\n", [Mod, F]),
                    ok;
                Error ->
                    io:format("Failed to compile ~s: ~p\n", [F, Error])
            end
        end,
    filelib:fold_files(Dir, ".*.erl", false, CompileFn, ok).

run_pre_hook() ->
    run_hook(basho_bench_config:get(pre_hook, no_op)).

run_post_hook() ->
    run_hook(basho_bench_config:get(post_hook, no_op)).

run_hook({Module, Function}) ->
    Module:Function();

run_hook(no_op) ->
    no_op.

%% just a utility, should be in basho_bench_utils.erl
%% but 's' is for multiple utilities, and so far this
%% is the only one.
md5(Bin) -> crypto:hash(md5, Bin).

%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2011 Gemini Mobile Technologies, Inc.  All rights reserved.
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(basho_bench_driver_hibarifs).

-export([init/0,
         new/1,
         run/4
        ]).

-export([runfun/2,
         run_brick_simple/0
        ]).

-include("basho_bench.hrl").

-record(state, { id, % Note: Worker id in *string*, not integer
                 client,
                 table,
                 proto,
                 basedir,
                 files = [],
                 filescnt = 0,
                 emptydirs = [],
                 emptydirscnt = 0,
                 dirname_gen
               }).

%% ====================================================================
%% API
%% ====================================================================

run_brick_simple() ->
    basho_bench_config:set(hibarifs_proto, brick_simple),
    init().

runfun(Op, Id) ->
    KeyGen = basho_bench_keygen:new(basho_bench_config:get(key_generator), Id),
    ValGen = basho_bench_valgen:new(basho_bench_config:get(value_generator), Id),
    {ok, State} = new(Id),
    runfun(Op, Id, KeyGen, ValGen, State).

runfun(Op, Id, KeyGen, ValGen, State) ->
    fun () ->
            case run(Op, KeyGen, ValGen, State) of
                {ok, NewState} ->
                    {ok, runfun(Op, Id, KeyGen, ValGen, NewState)};
                {error, Reason, NewState} ->
                    {{error, Reason}, runfun(Op, Id, KeyGen, ValGen, NewState)}
            end
    end.


%% init called only once per test
init() ->
    Node    = basho_bench_config:get(hibarifs_node, 'hibarifs@127.0.0.1'),
    Cookie  = basho_bench_config:get(hibarifs_cookie, 'hibari'),

    %% Try to spin up net_kernel
    MyNode  = basho_bench_config:get(hibarifs_mynode, [basho_bench, shortnames]),
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, {{already_started, _}, _}} ->
            %% TODO: doesn't match documentation
            ok;
        {error, Reason1} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason1])
    end,

    %% Initialize cookie for node
    true = erlang:set_cookie(Node, Cookie),

    %% Try to ping node
    ping(Node),

    %% Try to initialize the protocol-specific implementation
    Proto = basho_bench_config:get(hibarifs_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibarifs_table, tab1),
    ok = init(Proto, Table, Node),

    %% Initialize common objects
    {DirCount, _} = getopt_initial_file_count(),
    ok = init_dirs(0, DirCount, mount_dir()),

    %% done
    ok.

%% new called on each worker creation
new(Id) ->
    io:format("Initializing worker (id: ~p)\n", [Id]),

    Proto = basho_bench_config:get(hibarifs_proto, brick_simple_stub),
    {DirCount, FileCount} = getopt_initial_file_count(),

    %% Worker has a separate keygen for directory name generation.
    DirNameGen = basho_bench_keygen:new({truncated_pareto_int, DirCount - 1}, Id),

    %% Get client
    State = case Proto of
                localfs ->
                    #state { id = integer_to_list(Id),
                             basedir = mount_dir(),
                             dirname_gen = DirNameGen
                           };
                brick_simple_stub ->
                    Table  = basho_bench_config:get(hibarifs_table, tab1),
                    #state { id = integer_to_list(Id),
                             client = brick_simple,
                             table = Table,
                             proto = Proto,
                             basedir = mount_dir(),
                             dirname_gen = DirNameGen
                            };
                brick_simple ->  %% @TODO: Try not repeat the same code here
                    Table  = basho_bench_config:get(hibarifs_table, tab1),
                    #state { id = integer_to_list(Id),
                             client = brick_simple,
                             table = Table,
                             proto = Proto,
                             basedir = mount_dir(),
                             dirname_gen = DirNameGen
                            };
                _ ->
                    Reason1 = Proto,
                    ?FAIL_MSG("Failed to get a hibarifs client: ~p\n", [Reason1])
            end,

    io:format("Creating files"),

    ValGen = basho_bench_valgen:new(basho_bench_config:get(value_generator), Id),
    {ok, Files} = populate_dirs(0, DirCount, FileCount,
                                integer_to_list(Id), mount_dir(), ValGen, []),

    io:format("\nCreated total ~p files.\n", [length(Files)]),

    {ok, State#state{ files = Files, filescnt = length(Files) }}.

%% file operations
run(create=_Op, KeyGen, _ValGen,
    #state{id=Id, basedir=BaseDir, files=Files, filescnt=Cnt,
           dirname_gen=DirNameGen}=State) ->

    File = filename(Id, BaseDir, DirNameGen, KeyGen),
    case create_file(File) of
        ok ->
            case lists:member(File, Files) of
                true ->
                    {ok, State};
                false ->
                    {ok, State#state{files=[File|Files], filescnt=Cnt+1}}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end;
run(write=_Op, KeyGen, ValGen,
    #state{id=Id, basedir=BaseDir, files=Files, filescnt=0,
           dirname_gen=DirNameGen}=State) ->

    File = filename(Id, BaseDir, DirNameGen, KeyGen),
    case write_file(File, ValGen()) of
        ok ->
            {ok, State#state{files=[File|Files], filescnt=1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(write=_Op, _KeyGen, ValGen, #state{files=Files, filescnt=Cnt}=State) ->
    File = lists:nth(random:uniform(Cnt), Files),
    case write_file(File, ValGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rename=_Op, KeyGen, _ValGen,
    #state{id=Id, basedir=BaseDir, filescnt=0, dirname_gen=DirNameGen}=State) ->

    FileFrom = filename(Id, BaseDir, DirNameGen, KeyGen),
    FileTo   = FileFrom ++ "_renamed",
    case basho_bench_erlang_file_alternative:rename(FileFrom, FileTo) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rename=_Op, _KeyGen, _ValGen,
    #state{files=[FileFrom|Files], filescnt=Cnt}=State) ->
    FileTo = FileFrom ++ "_renamed",
    case basho_bench_erlang_file_alternative:rename(FileFrom, FileTo) of
        ok ->
            {ok, State#state{files=[FileTo|Files]}};
        {error, enoent} ->
            {error, ok, State#state{files=Files, filescnt=Cnt-1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(move=_Op, _KeyGen, _ValGen, _State) ->
    %% TODO: Implement move operation
    throw(unsupported_operation);
run(delete=_Op, KeyGen, _ValGen,
    #state{id=Id, basedir=BaseDir, filescnt=0, dirname_gen=DirNameGen}=State) ->

    File = filename(Id, BaseDir, DirNameGen, KeyGen),
    case basho_bench_erlang_file_alternative:delete(File) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete=_Op, _KeyGen, _ValGen, #state{files=[File|Files], filescnt=Cnt}=State) ->
    case basho_bench_erlang_file_alternative:delete(File) of
        ok ->
            {ok, State#state{files=Files, filescnt=Cnt-1}};
        {error, enoent} ->
            {error, ok, State#state{files=Files, filescnt=Cnt-1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(read=_Op, KeyGen, _ValGen,
    #state{id=Id, basedir=BaseDir, filescnt=0, dirname_gen=DirNameGen}=State) ->

    File = filename(Id, BaseDir, DirNameGen, KeyGen),
    case read_file(File) of
        {ok, _Count} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(read=_Op, _KeyGen, _ValGen, #state{files=Files, filescnt=Cnt}=State) ->
    File = lists:nth(random:uniform(Cnt), Files),
    case read_file(File) of
        {ok, _Count} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State#state{files=lists:delete(File, Files), filescnt=Cnt-1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% directory operations
run(lsdir=_Op, _KeyGen, _ValGen, #state{basedir=BaseDir, dirname_gen=DirNameGen}=State) ->
    Dir = dirname(BaseDir, DirNameGen),
    case file:list_dir(Dir) of
        {ok, _Filenames} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% empty directory operations
run(mkdir_empty=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    Dir = empty_dirname(BaseDir, KeyGen),
    case file:make_dir(Dir) of
        ok ->
            {ok, State};
        {error, eexist} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rmdir_empty=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    Dir = empty_dirname(BaseDir, KeyGen),
    case file:del_dir(Dir) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(lsdir_empty=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    Dir = empty_dirname(BaseDir, KeyGen),
    case file:list_dir(Dir) of
        {ok, _Filenames} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% special file operations
run(create_and_delete_topdir=_Op, KeyGen, _ValGen,
    #state{id=Id, basedir=BaseDir}=State) ->

    File = filename(Id, BaseDir, KeyGen),
    case create_file(File) of
        ok ->
            case file:delete(File) of
                ok ->
                    {ok, State};
                {error, enoent} ->
                    {error, ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end;
run(create_and_delete_subdir=_Op, KeyGen, _ValGen,
    #state{id=Id, basedir=BaseDir, dirname_gen=DirNameGen}=State) ->

    File = filename(Id, BaseDir, DirNameGen, KeyGen),
    case create_file(File) of
        ok ->
            case file:delete(File) of
                ok ->
                    {ok, State};
                {error, enoent} ->
                    {error, ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

%% TODO: rename dir operation


%% ====================================================================
%% Internal functions
%% ====================================================================

init(localfs=_Proto, _Table, _Node) ->
    io:format("init(localfs)\n"),
    file:make_dir(mount_dir());
init(brick_simple_stub=Proto, Table, Node) ->
    io:format("init(brick_simple_stub)\n"),
    HibariFS = hibarifs_fuse,
    HibariFSApp = "hibarifs_fuse.app",

    %% Start stub
    ok = rpc(Node, Proto, start, []),
    %% Start table
    ok = rpc(Node, Proto, start_table, [Table]),
    %% Wait for table
    ok = rpc(Node, Proto, wait_for_table, [Table]),

    %% Umount
    case rpc(Node, application, stop, [HibariFS]) of
        ok ->
            ok = rpc(Node, application, unload, [HibariFS]),
            ok;
        {error,{not_started,HibariFS}} ->
            ok
    end,

    %% Mount
    Dir = mount_dir(),
    ok = filelib:ensure_dir(Dir),
    EBinDir = rpc(Node, code, lib_dir, [HibariFS, ebin]),
    {ok, [App]} = rpc(Node, file, consult, [filename:join(EBinDir, HibariFSApp)]),
    _ = rpc(Node, application, load, [App]),
    ok = rpc(Node, application, set_env, [HibariFS, mount_point, Dir]),
    ok = rpc(Node, application, set_env, [HibariFS, mount_table, Table]),
    ok = rpc(Node, application, set_env, [HibariFS, mount_varprefixnum, 0]),
    ok = rpc(Node, application, start, [HibariFS]),

    %% done
    ok;
init(brick_simple=_Proto, Table, HibariFSNode) ->
    io:format("init(brick_simple, ~p, ~p)\n", [Table, HibariFSNode]),

    HibariNodes = basho_bench_config:get(hibari_admin_nodes, ['hibari@127.0.0.1']),
    HibariNode  = hd(HibariNodes),

    HibariFS = hibarifs_fuse,
    HibariFSApp = "hibarifs_fuse.app",

    %application:start(sasl),

    %% Make sure gmt_util is running
    case application:start(gmt_util) of
        ok ->
            ok;
        {error, {already_started, gmt_util}} ->
            ok;
        {error, Reason1} ->
            ?FAIL_MSG("Failed to start gmt_util for ~p: ~p\n", [?MODULE, Reason1])
    end,

    %% Make sure gdss_client is running
    case application:start(gdss_client) of
        ok ->
            ok;
        {error, {already_started, gdss_client}} ->
            ok;
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start gdss_client for ~p: ~p\n", [?MODULE, Reason2])
    end,

    %% Register client nodes to Hibari
    ok = rpc(HibariNode, brick_admin, add_client_monitor, [HibariFSNode]),
    ok = rpc(HibariNode, brick_admin, add_client_monitor, [node()]),

    wait_for_tables(HibariNode, [Table]),

    %% Check if the table exists
    case rpc(HibariNode, brick_admin, get_table_info, [Table]) of
        {ok, _} ->
            %% @TODO: CHECKME: There may be better way to check if table is empty?
            Keys = brick_simple:get_many(Table, <<>>, 1, [witness]),
            if length(Keys) > 0 ->
                    ?WARN("Table ~p is not empty.\n", [Table]);
               true ->
                    ok
            end;
        error ->
            ?FAIL_MSG("Table '~p' does not exist on Hibari ~p.\n",
                      [Table, HibariNode])
    end,

    %% Umount
    case rpc(HibariFSNode, application, stop, [HibariFS]) of
        ok ->
            ok = rpc(HibariFSNode, application, unload, [HibariFS]),
            ok;
        {error,{not_started,HibariFS}} ->
            ok
    end,

    %% Mount
    Dir = mount_dir(),
    ok = filelib:ensure_dir(Dir),
    EBinDir = rpc(HibariFSNode, code, lib_dir, [HibariFS, ebin]),
    {ok, [App]} = rpc(HibariFSNode, file, consult, [filename:join(EBinDir, HibariFSApp)]),
    _ = rpc(HibariFSNode, application, load, [App]),
    ok = rpc(HibariFSNode, application, set_env, [HibariFS, mount_point, Dir]),
    ok = rpc(HibariFSNode, application, set_env, [HibariFS, mount_table, Table]),
    ok = rpc(HibariFSNode, application, set_env, [HibariFS, mount_varprefixnum, 0]),
    ok = rpc(HibariFSNode, application, start, [HibariFS]),

    %% Un-register this node from Hibari
    ok = rpc(HibariNode, brick_admin, delete_client_monitor, [node()]),
    ok = application:stop(gdss_client),

    %% done
    ok;
init(Proto, _Table, _Node) ->
    ?FAIL_MSG("Unknown protocol for ~p: ~p\n", [?MODULE, Proto]).

init_dirs(N, N, _) ->
    io:format("Created ~p shared directories.\n", [N]),
    ok;
init_dirs(N, DirCount, BaseDir) ->
    Dir = dirname(BaseDir, N),
    case file:make_dir(Dir) of
        ok ->
            init_dirs(N + 1, DirCount, BaseDir);
        {error, Reason} ->
            ?WARN("Failed to create dir (~p): ~p\n", [Reason, Dir]),
            %% {error, Reason}
            init_dirs(N + 1, DirCount, BaseDir)
    end.

populate_dirs(N, N, _FileCount, _Id, _BaseDir, _ValGen, AllFiles) ->
    {ok, AllFiles};
populate_dirs(N, DirCount, FileCount, Id, BaseDir, ValGen, AllFiles) ->
    Dir = dirname(BaseDir, N),
    {ok, Files} = init_files(0, FileCount, Id, Dir, ValGen, []),

    populate_dirs(N + 1, DirCount, FileCount, Id, BaseDir, ValGen,
                  lists:append(AllFiles, Files)).

init_files(N, N, _FileCount, _Dir, _ValGen, Files) ->
    %% io:format("Created ~p files under ~p.\n", [N, _Dir]),
    io:format("."),
    {ok, Files};
init_files(N, FileCount, Id, Dir, ValGen, Files) ->
    File = filename(Id, Dir, N),
    case write_file(File, ValGen()) of
        ok ->
            init_files(N + 1, FileCount, Id, Dir, ValGen, [File|Files]);
        {error, Reason} ->
            {error, Reason}
    end.

ping(Node) ->
    case net_adm:ping(Node) of
        pong ->
            ok;
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.

rpc(Node, M, F, A) ->
    %% io:format("rpc(~p, ~p, ~p, ~p)\n", [Node, M, F, A]),
    case rpc:call(Node, M, F, A, 15000) of
        {badrpc, Reason} ->
            ?FAIL_MSG("RPC(~p:~p:~p) to ~p failed: ~p\n",
                      [M, F, A, Node, Reason]);
        Reply ->
            Reply
    end.

mount_dir() ->
    {ok, Cwd} = file:get_cwd(),
    filename:join([Cwd, ?MODULE_STRING ++ "." ++ atom_to_list(node())]).


% TODO we still want to measure performance of is_dir

%ensure_dirfile(BaseDir, KeyGen) ->
%    %Dir = dirname(BaseDir, KeyGen),
%    Dir = filename:join(BaseDir, "large_1"),
%    case filelib:is_dir(Dir) of
%        false ->
%            case file:make_dir(Dir) of
%                ok ->
%                    ok;
%                {error, eexist} ->
%                    ok;
%                {error, Reason} ->
%                    exit({ensure_dirfile, Dir, Reason})
%            end;
%        true ->
%            ok
%    end,
%    filename(Dir, KeyGen).

dirname(BaseDir, N) when is_integer(N) ->
    Dir = integer_to_list(N),
    filename:join([BaseDir, Dir]);
dirname(BaseDir, DirNameGen) ->
    dirname(BaseDir, DirNameGen()).

empty_dirname(BaseDir, DirNameGen) ->
    Dir = integer_to_list(DirNameGen()),
    Empty = "$",
    EmptyDir = Dir ++ Empty,
    filename:join([BaseDir, EmptyDir]).

filename(Id, N) when is_integer(N)->
    Id ++ "_" ++ integer_to_list(N).

filename(Id, Dir, N) when is_integer(N) ->
    filename:join([Dir, filename(Id, N)]);
filename(Id, Dir, KeyGen) ->
    filename:join([Dir, filename(Id, KeyGen())]).

filename(Id, BaseDir, DirNameGen, KeyGen) ->
    Dir = dirname(BaseDir, DirNameGen),
    filename(Id, Dir, KeyGen).


create_file(Filename) ->
    case file:open(Filename, [raw,write,binary]) of
        {ok, FD} ->
            file:close(FD);
        Err ->
            Err
    end.

write_file(Filename, Data) ->
    case file:open(Filename, [raw,write,binary]) of
        {ok, FD} ->
            case file:write(FD, Data) of
                ok ->
                    file:close(FD);
                Err ->
                    file:close(FD),
                    Err
            end;
        Err ->
            Err
    end.

read_file(Filename) ->
    case file:open(Filename, [raw,read,binary]) of
        {ok, FD} ->
            read_file_fully(FD);
        Err ->
            Err
    end.

read_file_fully(FD) ->
    read_file_fully(FD, 0).

read_file_fully(FD, N) ->
    %% @TODO should this value of 128KB be larger or smaller ?
    case file:read(FD, 131072) of
        {ok, Data} ->
            read_file_fully(FD, N + byte_size(Data));
        eof ->
            case file:close(FD) of
                ok ->
                    {ok, N};
                Err ->
                    Err
            end;
        Err ->
            file:close(FD),
            Err
    end.

getopt_initial_file_count() ->
    Option = basho_bench_config:get(initial_file_count, {{dir, 50}, {file, 20}}),
    {{dir, DirCount}, {file, FileCount}} = Option,

    if
        DirCount =< 0 ->
            ?FAIL_MSG("Illegal \"dir\" value for initial_file_count. " ++
                          "It must be > 0. ~p\n", [Option]);
        FileCount < 0 ->
            ?FAIL_MSG("Illegal \"file\" value for initial_file_count. " ++
                          "It must be >= 0. ~p\n", [Option]);
        true ->
            {DirCount, FileCount}
    end.


%% @TODO: MOVEME: Move Hibari client related functions to a separate utility module

wait_for_tables(GDSSAdmin, Tables) ->
    _ = [ ok = gmt_loop:do_while(fun poll_table/1, {GDSSAdmin,not_ready,Tab})
          || {Tab,_,_} <- Tables ],
    ok.

poll_table({GDSSAdmin,not_ready,Tab} = T) ->
    TabCh = gmt_util:atom_ify(gmt_util:list_ify(Tab) ++ "_ch1"),
    case rpc:call(GDSSAdmin, brick_sb, get_status, [chain, TabCh]) of
        {ok, healthy} ->
            {false, ok};
        _ ->
            ok = timer:sleep(50),
            {true, T}
    end.

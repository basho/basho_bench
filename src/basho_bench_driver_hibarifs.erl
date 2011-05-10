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

-export([runfun/2,
         init/0,
         new/1,
         run/4]).

-include("basho_bench.hrl").

-define(DEBUG, true).
%-undef(DEBUG).

-ifdef(DEBUG).

-define(FILENAME(X), 
        string:substr(X, length(State#state.basedir) + 1)).

%-define(TRACE_OPE(Ope, Var),
%        BaseDirLen = length(State#state.basedir),
%        io:format("~s(~p)~n", [Ope, string:substr(Var, BaseDirLen + 1)])).
-define(TRACE_OPE(Ope, Var), true).
-define(TRACE_LSDIR(Dir, Filenames), 
        io:format("lsdir(~p) => ~p~n", [?FILENAME(Dir), Filenames])).
-define(TRACE_LSDIR_ERR(Dir, Error), 
        io:format("lsdir(~p) => ~p~n", [?FILENAME(Dir), Error])).

-else.

-define(TRACE_OPE(Ope, Var), true).
-define(TRACE_LSDIR(Filename), true).
-define(TRACE_LSDIR_ERR(Error), true).

-endif.

-record(state, { client,
                 table,
                 proto,
                 basedir,
                 files = [],
                 filescnt = 0,
                 largedirs = [],
                 emptydirs = [],
                 emptydirscnt = 0
               }).

%% ====================================================================
%% API
%% ====================================================================

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
    io:format("init() called.~n"),
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
    init(Proto, Table, Node).

%% new called on each worker creation
new(Id) ->
    io:format("new(~p) called.~n", [Id]),
    Proto = basho_bench_config:get(hibarifs_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibarifs_table, tab1),

    %% Get a client
    case Proto of
        brick_simple_stub ->
            State =  #state { client = brick_simple,
                              table = Table,
                              proto = Proto,
                              basedir = mount_dir()
                            },
            {ok, _Dirs} = populate_large_dir(Id, State),
            {ok, State};
        local ->
            State = #state { basedir = "/tmp/hibaribench" },
            {ok, _Dirs} = populate_large_dir(Id, State),
            {ok, State};
        _ ->
            Reason1 = Proto,
            ?FAIL_MSG("Failed to get a hibarifs client: ~p\n", [Reason1])
    end.

%% file operations
run(create=_Op, KeyGen, _ValGen, #state{basedir=BaseDir, files=Files, filescnt=Cnt}=State) ->
    File = ensure_dirfile(BaseDir, KeyGen),
    ?TRACE_OPE(_Op, File),
    case file:write_file(File, <<>>) of
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
run(write=_Op, KeyGen, ValGen, #state{basedir=BaseDir, files=Files, filescnt=0}=State) ->
    File = ensure_dirfile(BaseDir, KeyGen),
    ?TRACE_OPE(_Op, File),
    case file:write_file(File, ValGen()) of
        ok ->
            {ok, State#state{files=[File|Files], filescnt=1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(write=_Op, _KeyGen, ValGen, #state{files=[File|Files]}=State) ->
    ?TRACE_OPE(_Op, File),
    case file:write_file(File, ValGen()) of
        ok ->
            {ok, State#state{files=lists:append(Files, [File])}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete=_Op, KeyGen, _ValGen, #state{basedir=BaseDir, filescnt=0}=State) ->
    Dir = dirname(BaseDir, KeyGen),
    File = filename(Dir, KeyGen),
    ?TRACE_OPE(_Op, File),
    case file:delete(File) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete=_Op, _KeyGen, _ValGen, #state{files=[File|Files], filescnt=Cnt}=State) ->
    ?TRACE_OPE(_Op, File),
    case file:delete(File) of
        ok ->
            {ok, State#state{files=Files, filescnt=Cnt-1}};
        {error, enoent} ->
            {error, ok, State#state{files=Files, filescnt=Cnt-1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(read=_Op, KeyGen, _ValGen, #state{basedir=BaseDir, filescnt=0}=State) ->
    Dir = dirname(BaseDir, KeyGen),
    File = filename(Dir, KeyGen),
    ?TRACE_OPE(_Op, File),
    case file:read_file(File) of
        {ok, _Binary} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(read=_Op, _KeyGen, _ValGen, #state{files=[File|Files], filescnt=Cnt}=State) ->
    ?TRACE_OPE(_Op, File),
    case file:read_file(File) of
        {ok, _Binary} ->
            {ok, State#state{files=lists:append(Files, [File])}};
        {error, enoent} ->
            {error, ok, State#state{files=Files, filescnt=Cnt-1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% directory operations
run(lsdir=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    %Dir = dirname(BaseDir, KeyGen),
    Dir =  filename:join(State#state.basedir, "large_1"),
    ?TRACE_OPE(_Op, Dir),
    case file:list_dir(Dir) of
        {ok, _Filenames} ->
            %?TRACE_LSDIR(Dir, _Filenames), 
            {ok, State};
        {error, enoent} ->
            %?TRACE_LSDIR_ERR(Dir, enoent),
            {error, ok, State};
        {error, Reason} ->
            %?TRACE_LSDIR_ERR(Dir, Reason),
            {error, Reason, State}
    end;
%% empty directory operations
run(mkdir_empty=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    Dir = empty_dirname(BaseDir, KeyGen),
    ?TRACE_OPE(_Op, Dir),
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
    ?TRACE_OPE(_Op, Dir),
    case file:del_dir(Dir) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(lsdir_empty=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    %Dir = empty_dirname(BaseDir, KeyGen),
    Dir = 
    ?TRACE_OPE(_Op, Dir),
    case file:list_dir(Dir) of
        {ok, _Filenames} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% special file operations
run(create_and_delete_topdir=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    File = filename(BaseDir, KeyGen),
    ?TRACE_OPE(_Op, File),
    case file:write_file(File, <<>>) of
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
run(create_and_delete_subdir=_Op, KeyGen, _ValGen, #state{basedir=BaseDir}=State) ->
    File = ensure_dirfile(BaseDir, KeyGen),
    ?TRACE_OPE(_Op, File),
    case file:write_file(File, <<>>) of
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


%% ====================================================================
%% Internal functions
%% ====================================================================

init(brick_simple_stub=Proto, Table, Node) ->
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
init(local=Proto, _Table, _Node) ->
    Dir = "/tmp/hibaribench",
    ok = filelib:ensure_dir(Dir),
    ok;
init(Proto, _Table, _Node) ->
    ?FAIL_MSG("Unknown protocol for ~p: ~p\n", [?MODULE, Proto]).

populate_large_dir(Id, State) ->
    FileCount = 100, % TODO: Set this value via config.
    Dir = filename:join(State#state.basedir, "large_" ++ integer_to_list(Id)),
    ok =  file:make_dir(Dir),
    ok = create_file(0, FileCount, Dir),

    %{ok, FileNames} =  file:list_dir(Dir),
    %io:format("~p~n", [FileNames]),

    {ok, [Dir]}.

create_file(X, X, _) ->
    io:format("Created ~p files.~n", [X]),
    ok;
create_file(I, Max, Dir) ->
    File = filename:join(Dir, integer_to_list(I)), 
    case file:write_file(File, <<>>) of 
        ok -> 
            create_file(I + 1, Max, Dir); 
        _ -> 
            io:format("ERROR!!!!!!~n"),
            {error, cant_create_file}
    end.            

ping(Node) ->
    case net_adm:ping(Node) of
        pong ->
            ok;
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.

rpc(Node, M, F, A) ->
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

ensure_dirfile(BaseDir, KeyGen) ->
    %Dir = dirname(BaseDir, KeyGen),
    Dir = filename:join(BaseDir, "large_1"),
    case filelib:is_dir(Dir) of
        false ->
            case file:make_dir(Dir) of
                ok ->
                    ok;
                {error, eexist} ->
                    ok;
                {error, Reason} ->
                    exit({ensure_dirfile, Dir, Reason})
            end;
        true ->
            ok
    end,
    filename(Dir, KeyGen).

dirname(BaseDir, KeyGen) ->
    Dir = integer_to_list(KeyGen()),
    filename:join([BaseDir, Dir]).

empty_dirname(BaseDir, KeyGen) ->
    Dir = integer_to_list(KeyGen()),
    Empty = "$",
    EmptyDir = Dir ++ Empty,
    filename:join([BaseDir, EmptyDir]).

filename(KeyGen) ->
    integer_to_list(KeyGen()).

filename(Dir, KeyGen) ->
    filename:join([Dir, filename(KeyGen)]).

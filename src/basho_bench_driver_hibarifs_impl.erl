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
-module(basho_bench_driver_hibarifs_impl).

-export([runfun/2,
         init/0,
         new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {table,
                verprefixnum=2,
                timeout=5000,
                generation=0,
                major_device=0,
                minor_device=0,
                dirmode=8#00775,
                filemode=8#00664,
                attr_timeout=1000,
                entry_timeout=1000,
                cache_timeout=5000
               }).

-record(info, {ino,
               ino_pid=undefined,
               parent,
               parent_pid=undefined,
               name,
               path,
               type,
               timestamp,
               size
              }).

-define(RPC_TIMEOUT, 15000).

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

init() ->
    %% Try to spin up net_kernel
    MyNode  = basho_bench_config:get(hibari_mynode, [basho_bench, shortnames]),
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

    %% Try to initialize the protocol-specific implementation
    Proto = basho_bench_config:get(hibarifs_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibarifs_table, tab1),
    init(Proto, Table).

new(_Id) ->
    Proto = basho_bench_config:get(hibarifs_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibarifs_table, tab1),

    %% Get a client
    case Proto of
        brick_simple_stub ->
            {ok, #state { table = Table }};
        brick_simple ->
            {ok, #state { table = Table }};
        _ ->
            Reason1 = Proto,
            ?FAIL_MSG("Failed to get a hibari client: ~p\n", [Reason1])
    end.

%% file operations
run(getinfo=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Parent = unused,
    ParentPid = unused,
    Name = filename(KeyGen),
    Path = dirname(KeyGen),
    case hibarifs_impl:get_info(Txn, Parent, ParentPid, Name, Path, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(listdir=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Ino = 0,
    Info = info_dir(KeyGen),
    Parent = 0,
    ParentInfo = info_dir(KeyGen),
    case hibarifs_impl:ls_dir(Txn, Ino, Info, Parent, ParentInfo, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(makedirnod=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Path = dirname(KeyGen),
    Name = dirname(KeyGen),
    Type = directory,
    case hibarifs_impl:mk_nod(Txn, Path, Name, Type, State) of
        {ok, _, _, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(makefilenod=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Path = dirname(KeyGen),
    Name = filename(KeyGen),
    Type = regular,
    case hibarifs_impl:mk_nod(Txn, Path, Name, Type, State) of
        {ok, _, _, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(openfile=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Ino = 0,
    Info = info_file(KeyGen),
    Modes = [],
    case hibarifs_impl:open_file(Txn, Ino, Info, Modes, State) of
        {ok, _NewTs, _Buf, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(renamefile=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Info = info_file(KeyGen),
    NewPath = dirname(KeyGen),
    case hibarifs_impl:rename_file(Txn, Info, NewPath, State) of
        {ok, _, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rmfile=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Info = info_file(KeyGen),
    case hibarifs_impl:rm_file(Txn, Info, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rmdir=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Info = info_dir(KeyGen),
    case hibarifs_impl:rm_dir(Txn, Info, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(writefile=_Op, KeyGen, ValGen, State) ->
    Txn = txn(),
    Info = info_file(KeyGen),
    Buf = ValGen(),
    case hibarifs_impl:write_file(Txn, Info, Buf, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

init(brick_simple_stub=Proto, Table) ->
    %% Make sure the path is setup such that we can get at brick_simple_stub
    case code:which(Proto) of
        non_existing ->
            ?FAIL_MSG("~p requires ~p module to be available on code path.\n",
                      [?MODULE, Proto]);
        _ ->
            ok
    end,

    %% Make sure gmt_util is running
    case application:start(gmt_util) of
        ok ->
            ok;
        {error, {already_started,gmt_util}} ->
            ok;
        {error, Reason1} ->
            ?FAIL_MSG("Failed to start gmt_util for ~p: ~p\n", [?MODULE, Reason1])
    end,

    %% Make sure gdss_client is running
    case application:start(gdss_client) of
        ok ->
            ok;
        {error, {already_started,gdss_client}} ->
            ok;
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start gdss_client for ~p: ~p\n", [?MODULE, Reason2])
    end,

    ok = Proto:start(),
    ok = Proto:start_table(Table),
    Proto:wait_for_table(Table);
init(brick_simple=Proto, Table) ->
    io:format("init(brick_simple, ~p)\n", [Table]),
    HibariNodes = basho_bench_config:get(hibari_admin_nodes, ['hibari@127.0.0.1']),
    HibariNode  = hd(HibariNodes),

    %% Make sure the path is setup such that we can get at brick_simple_stub
    case code:which(Proto) of
        non_existing ->
            ?FAIL_MSG("~p requires ~p module to be available on code path.\n",
                      [?MODULE, Proto]);
        _ ->
            ok
    end,

    %% Make sure gmt_util is running
    case application:start(gmt_util) of
        ok ->
            ok;
        {error, {already_started,gmt_util}} ->
            ok;
        {error, Reason1} ->
            ?FAIL_MSG("Failed to start gmt_util for ~p: ~p\n", [?MODULE, Reason1])
    end,

    %% Make sure gdss_client is running
    case application:start(gdss_client) of
        ok ->
            ok;
        {error, {already_started,gdss_client}} ->
            ok;
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start gdss_client for ~p: ~p\n", [?MODULE, Reason2])
    end,

    %% Register client nodes to Hibari
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

    %% done
    ok;
init(Proto, _Table) ->
    ?FAIL_MSG("Unknown protocol for ~p: ~p\n", [?MODULE, Proto]).

dirname(KeyGen) ->
    list_to_binary(integer_to_list(KeyGen())).

filename(KeyGen) ->
    list_to_binary(integer_to_list(KeyGen())).

info_file(KeyGen) ->
    #info{ino=undefined, parent=undefined, name=filename(KeyGen), path=dirname(KeyGen),
          type=regular, timestamp=0, size=0
         }.

info_dir(KeyGen) ->
    #info{ino=undefined, parent=undefined, name=filename(KeyGen), path=dirname(KeyGen),
          type=directory, timestamp=0, size=0
         }.

txn() ->
    {{fuse_ctx, undefined, undefined, undefined}, undefined, undefined, undefined}.

rpc(Node, M, F, A) ->
    %% io:format("rpc(~p, ~p, ~p, ~p)\n", [Node, M, F, A]),
    case rpc:call(Node, M, F, A, ?RPC_TIMEOUT) of
        {badrpc, Reason} ->
            ?FAIL_MSG("RPC(~p:~p:~p) to ~p failed: ~p\n",
                      [M, F, A, Node, Reason]);
        Reply ->
            Reply
    end.

%% @TODO: MOVEME: Move Hibari client related functions to a separate utility module

wait_for_tables(GDSSAdmin, Tables) ->
    _ = [ ok = gmt_loop:do_while(fun poll_table/1, {GDSSAdmin,not_ready,Tab})
          || {Tab,_,_} <- Tables ],
    ok.

poll_table({GDSSAdmin,not_ready,Tab} = T) ->
    TabCh = gmt_util:atom_ify(gmt_util:list_ify(Tab) ++ "_ch1"),
    case rpc:call(GDSSAdmin, brick_sb, get_status, [chain, TabCh], ?RPC_TIMEOUT) of
        {ok, healthy} ->
            {false, ok};
        _ ->
            ok = timer:sleep(50),
            {true, T}
    end.

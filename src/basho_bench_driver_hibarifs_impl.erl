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
                client,
                timeout=5000,
                proto,
                unused3,
                unused4,
                dirmode=0,
                filemode=0,
                unused7,
                unused8,
                unused9,
                unused10,
                unused11
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

init() ->
    %% Try to spin up net_kernel
    MyNode  = basho_bench_config:get(hibari_mynode, [basho_bench, shortnames]),
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, {{already_started, _}}, _} ->
            %% TODO: doesn't match documentation
            ok;
        {error, Reason1} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason1])
    end,

    %% Try to initialize the protocol-specific implementation
    Proto = basho_bench_config:get(hibari_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibari_table, tab1),
    init(Proto, Table).

new(_Id) ->
    Proto = basho_bench_config:get(hibari_proto, brick_simple_stub),
    Table  = basho_bench_config:get(hibari_table, tab1),

    %% Get a client
    case Proto of
        brick_simple_stub ->
            {ok, #state { client = brick_simple,
                          table = Table,
                          proto = Proto }};
        _ ->
            Reason1 = Proto,
            ?FAIL_MSG("Failed to get a hibari client: ~p\n", [Reason1])
    end.

%% file operations
run(getinfo=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Parent = unused,
    Name = filename(KeyGen),
    Path = dirname(KeyGen),
    case hibarifs_fuse:get_info(Txn, Parent, Name, Path, State) of
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
    case hibarifs_fuse:list_dir(Txn, Ino, Info, Parent, ParentInfo, State) of
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
    case hibarifs_fuse:make_nod(Txn, Path, Name, Type, State) of
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
    case hibarifs_fuse:make_nod(Txn, Path, Name, Type, State) of
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
    case hibarifs_fuse:open_file(Txn, Ino, Info, Modes, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(renamefile=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Info = info_file(KeyGen),
    NewPath = dirname(KeyGen),
    case hibarifs_fuse:rename_file(Txn, Info, NewPath, State) of
        {ok, _, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rmfile=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Info = info_file(KeyGen),
    case hibarifs_fuse:rm_file(Txn, Info, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rmdir=_Op, KeyGen, _ValGen, State) ->
    Txn = txn(),
    Info = info_dir(KeyGen),
    case hibarifs_fuse:rm_dir(Txn, Info, State) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(writefile=_Op, KeyGen, ValGen, State) ->
    Txn = txn(),
    Info = info_file(KeyGen),
    Buf = ValGen(),
    case hibarifs_fuse:write_file(Txn, Info, Buf, State) of
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
init(Proto, _Table) ->
    ?FAIL_MSG("Unknown protocol for ~p: ~p\n", [?MODULE, Proto]).

dirname(KeyGen) ->
    list_to_binary(integer_to_list(KeyGen())).

filename(KeyGen) ->
    list_to_binary(integer_to_list(KeyGen())).

info_file(KeyGen) ->
    {info, undefined, undefined, filename(KeyGen), dirname(KeyGen), regular, undefined, undefined}.

info_dir(KeyGen) ->
    {info, undefined, undefined, filename(KeyGen), dirname(KeyGen), directory, undefined, undefined}.

txn() ->
    {{fuse_ctx, undefined, undefined, undefined}, undefined, undefined, undefined}.


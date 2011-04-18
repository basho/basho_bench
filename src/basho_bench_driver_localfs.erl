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
-module(basho_bench_driver_localfs).

-export([init/0,
         new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { basedir }).

%% ====================================================================
%% API
%% ====================================================================

init() ->
    %% Mount
    Dir = mount_dir(),
    ok = filelib:ensure_dir(Dir),

    %% done
    ok.

new(_Id) ->
    {ok, #state { basedir = mount_dir() }}.

%% file operations
run(create=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    File = ensure_dirfile(BaseDir, KeyGen),
    case file:write_file(File, <<>>) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(write=_Op, KeyGen, ValueGen, #state{basedir=BaseDir}=State) ->
    File = ensure_dirfile(BaseDir, KeyGen),
    case file:write_file(File, ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    Dir = dirname(BaseDir, KeyGen),
    File = filename(Dir, KeyGen),
    case file:delete(File) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(read=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    Dir = dirname(BaseDir, KeyGen),
    File = filename(Dir, KeyGen),
    case file:read_file(File) of
        {ok, _Binary} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% directory operations
run(lsdir=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    Dir = dirname(BaseDir, KeyGen),
    case file:list_dir(Dir) of
        {ok, _Filenames} ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% empty directory operations
run(mkdir_empty=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    Dir = empty_dirname(BaseDir, KeyGen),
    case file:make_dir(Dir) of
        ok ->
            {ok, State};
        {error, eexist} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(rmdir_empty=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    Dir = empty_dirname(BaseDir, KeyGen),
    case file:del_dir(Dir) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {error, ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(lsdir_empty=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
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
run(create_and_delete_topdir=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    File = filename(BaseDir, KeyGen),
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
run(create_and_delete_subdir=_Op, KeyGen, _ValueGen, #state{basedir=BaseDir}=State) ->
    File = ensure_dirfile(BaseDir, KeyGen),
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

mount_dir() ->
    filename:join(["/", "mnt", "ramdisk1", ?MODULE_STRING]).

ensure_dirfile(BaseDir, KeyGen) ->
    Dir = dirname(BaseDir, KeyGen),
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

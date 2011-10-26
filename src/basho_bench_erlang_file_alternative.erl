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
-module(basho_bench_erlang_file_alternative).

%% By default, Erlang/OTP file operations are implemented by a centralized io
%% server process.  This io server process is a bottleneck for benchmarking
%% purposes.
%%
%% This module implements the file operations without using the centralized 
%% io server.


-export([make_dir/1,
         del_dir/1,
         list_dir/1,
         delete/1,
         rename/2
         % setattr/2
        ]).


make_dir(Dir) ->
    do_file_op(make_dir, Dir).

del_dir(Dir) ->
    do_file_op(del_dir, Dir).

list_dir(Dir) ->
    do_file_op(list_dir, Dir).

rename(From, To) ->
    do_file_op(rename, From, To).

delete(File) ->
    do_file_op(delete, File).

%delete(File) ->
%    case file:open(Filename, [raw,write,binary]) of
%        {ok, {file_descriptor, Module, {Port, _FileNo}}} ->
%            Module:delete(Port, Filename);
%            %% file:close(FD);  %%% <------ Doesn't work.
%        Err ->
%            Err
%    end.


do_file_op(FunName, Param1) ->
    case prim_file_drv_open(efile, [binary]) of
        {ok, Port} ->
            case prim_file:FunName(Port, Param1) of
                ok ->
                    prim_file_drv_close(Port);
                Err ->
                    prim_file_drv_close(Port),
                    Err
            end;
        Err ->
            Err
    end.

do_file_op(FunName, Param1, Param2) ->
    case prim_file_drv_open(efile, [binary]) of
        {ok, Port} ->
            case prim_file:FunName(Port, Param1, Param2) of
                ok ->
                    prim_file_drv_close(Port);
                Err ->
                    prim_file_drv_close(Port),
                    Err
            end;
        Err ->
            Err
    end.


%%%-----------------------------------------------------------------
%%% Functions to communicate with the erts *prim_file* driver
%%% (Copied from erts/prim_file.erl)

%% Opens a driver port and converts any problems into {error, emfile}.
%% Returns {ok, Port} when succesful.

prim_file_drv_open(Driver, Portopts) ->
    try erlang:open_port({spawn, Driver}, Portopts) of
	Port ->
	    {ok, Port}
    catch
	error:Reason ->
	    {error, Reason}
    end.


%% Closes a port in a safe way. Returns ok.

prim_file_drv_close(Port) ->
    try erlang:port_close(Port) catch error:_ -> ok end,
    receive %% Ugly workaround in case the caller==owner traps exits
	{'EXIT', Port, _Reason} -> 
	    ok
    after 0 -> 
	    ok
    end.

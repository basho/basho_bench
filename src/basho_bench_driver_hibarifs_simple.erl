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
-module(basho_bench_driver_hibarifs_simple).

-export([init/0,
         new/1,
         run/4
        ]).


-include("basho_bench.hrl").

-record(state, { id, % Note: Worker id in *string*, not integer
                 basedir,
                 opecount   = 0,
                 successful = 0, 
                 failed     = 0,
                 maxfiles,
                 starttime  = undefined,
                 debug
               }).

-define(DRIVER_DEBUG_MSG(State, Op, Result, File),
        case State#state.debug of
            true ->
                ?DEBUG("Worker ~p: ~p (~p) ~p\n", 
                       [list_to_integer(State#state.id), Op, Result, File]);
            _ ->
                noop
        end).

%% ====================================================================
%% API
%% ====================================================================

%% init called only once per test
init() ->
    BaseDir = mount_dir(),
    case filelib:is_file(BaseDir) of
        true ->
            ok;
        false ->
            ?FAIL_MSG("Can't access to the hibarifs_mount_point: ~p\n", [BaseDir])
    end.


%% new called on each worker creation
new(Id) ->
    io:format("Initializing worker (id: ~p)\n", [Id]),
    BaseDir = mount_dir(),
    MaxFiles = basho_bench_config:get(max_files_per_worker, 5000),
    Debug    = basho_bench_config:get(debug, false),
    State = #state { id = integer_to_list(Id),
                     basedir = BaseDir,
                     maxfiles = MaxFiles,
                     debug = Debug
                   },

    Dir = dirname(BaseDir, Id),
    Result = file:make_dir(Dir),
    ?DRIVER_DEBUG_MSG(State, mkdir, Result, Dir),

    {ok, State}.


%% file operations
run(Op, KeyGen, ValGen, #state{starttime=undefined}=State) ->
    run(Op, KeyGen, ValGen, State#state{starttime=now()});
run(Op, _, _, #state{opecount=Count, maxfiles=Max}=State) when Count >= Max ->
    ElapseMillis = timer:now_diff(now(), State#state.starttime) / 1000.0,
    case Op of
        delete ->
            Dir = dirname(State#state.basedir, State#state.id),
            Result = file:del_dir(Dir),
            ?DRIVER_DEBUG_MSG(State, rmdir, Result, Dir);
        _ ->
            noop
    end,

    {stop, {finished,
            {worker_id,     list_to_integer(State#state.id)},
            {elapse_millis, ElapseMillis},
            {total,         Count},
            {successful,    State#state.successful},
            {failed,        State#state.failed} 
           }
    };
run(write=Op, _KeyGen, ValGen,
    #state{id=Id, basedir=BaseDir, opecount=Count, successful=Successful, failed=Failed}=State) ->
    File = filename(Id, BaseDir, Id, Count),
    case write_file(File, ValGen()) of
        ok ->
            ?DRIVER_DEBUG_MSG(State, Op, ok, File),
            {ok, State#state{opecount=Count+1, successful=Successful+1}};
        {error, Reason} ->
            ?DRIVER_DEBUG_MSG(State, Op, error, File),
            {error, Reason, State#state{opecount=Count+1, failed=Failed+1}}
    end;
run(read=Op, _KeyGen, _ValGen, 
    #state{id=Id, basedir=BaseDir, opecount=Count, successful=Successful, failed=Failed}=State) ->
    File = filename(Id, BaseDir, Id, Count),
    case read_file(File) of
        {ok, _Count} ->
            ?DRIVER_DEBUG_MSG(State, Op, ok, File),
            {ok, State#state{opecount=Count+1, successful=Successful+1}};
        {error, Reason} ->
            ?DRIVER_DEBUG_MSG(State, Op, error, File),
            {error, Reason, State#state{opecount=Count+1, failed=Failed+1}}
    end;
run(delete=Op, _KeyGen, _ValGen, 
    #state{id=Id, basedir=BaseDir, opecount=Count, successful=Successful, failed=Failed}=State) ->
    File = filename(Id, BaseDir, Id, Count),
    case basho_bench_erlang_file_alternative:delete(File) of
        ok ->
            ?DRIVER_DEBUG_MSG(State, Op, ok, File),
            {ok, State#state{opecount=Count+1, successful=Successful+1}};
        {error, Reason} ->
            ?DRIVER_DEBUG_MSG(State, Op, error, File),
            {error, Reason, State#state{opecount=Count+1, failed=Failed+1}}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

mount_dir() ->
    basho_bench_config:get(hibarifs_mount_point).

dirname(BaseDir, N) when is_list(N) ->
    filename:join([BaseDir, N]);
dirname(BaseDir, N) when is_integer(N) ->
    dirname(BaseDir, integer_to_list(N)).

filename(Id, N) when is_integer(N)->
    Id ++ "_" ++ integer_to_list(N).

filename(Id, Dir, N) when is_integer(N) ->
    filename:join([Dir, filename(Id, N)]).

filename(Id, BaseDir, DirNameGen, KeyGen) ->
    Dir = dirname(BaseDir, DirNameGen),
    filename(Id, Dir, KeyGen).


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




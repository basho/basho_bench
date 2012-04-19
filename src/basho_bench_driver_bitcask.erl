%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
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
-module(basho_bench_driver_bitcask).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { file,
                 filename,
                 flags,
                 sync_interval,
                 last_sync }).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Make sure bitcask is available
    case code:which(bitcask) of
        non_existing ->
            ?FAIL_MSG("~s requires bitcask to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    %% Get the target directory
    Dir = basho_bench_config:get(bitcask_dir, "."),
    Filename = filename:join(Dir, "test.bitcask"),

    %% Look for sync interval config
    case basho_bench_config:get(bitcask_sync_interval, infinity) of
        Value when is_integer(Value) ->
            SyncInterval = Value;
        infinity ->
            SyncInterval = infinity
    end,

    %% Get any bitcask flags
    Flags = basho_bench_config:get(bitcask_flags, []),
    case bitcask:open(Filename, [read_write] ++ Flags) of
        {error, Reason} ->
            ?FAIL_MSG("Failed to open bitcask in ~s: ~p\n", [Filename, Reason]);
        File ->
            %% Try to start the merge worker
            bitcask_merge_worker:start_link(),
            {ok, #state { file = File,
                          filename = Filename,
                          flags = Flags,
                          sync_interval = SyncInterval,
                          last_sync = os:timestamp() }}
    end.



run(get, KeyGen, _ValueGen, State) ->
    State1 = maybe_sync(State),
    case bitcask:get(State1#state.file, KeyGen()) of
        {ok, _Value} ->
            {ok, State1};
        not_found ->
            {ok, State1};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValueGen, State) ->
    State1 = maybe_sync(State),
    case bitcask:put(State1#state.file, KeyGen(), ValueGen()) of
        ok ->
            {ok, State1};
        {error, Reason} ->
            {error, Reason}
    end;
run(merge, _KeyGen, _ValueGen, State) ->
    case bitcask:needs_merge(State#state.file) of
        {true, Files} ->
            case bitcask_merge_worker:merge(State#state.filename, State#state.flags,
                                            Files) of
                ok ->
                    {ok, State};
                Other ->
                    {error, {merge_failed, Other}}
            end;
        false ->
            {ok, State}
    end.



maybe_sync(#state { sync_interval = infinity } = State) ->
    State;
maybe_sync(#state { sync_interval = SyncInterval } = State) ->
    Now = os:timestamp(),
    case timer:now_diff(Now, State#state.last_sync) / 1000000 of
        Value when Value >= SyncInterval ->
            bitcask:sync(State#state.file),
            State#state { last_sync = Now };
        _ ->
            State
    end.


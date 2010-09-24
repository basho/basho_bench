%% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_pb: Driver for riak protocol buffers client
%%
%% Copyright (c) 2009 Basho Techonologies
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
-module(basho_bench_driver_riakc_pb).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 r,
                 w,
                 dw,
                 rw}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    Replies = basho_bench_config:get(riakc_pb_replies, 2),
    R = basho_bench_config:get(riakc_pb_r, Replies),
    W = basho_bench_config:get(riakc_pb_w, Replies),
    DW = basho_bench_config:get(riakc_pb_dw, Replies),
    RW = basho_bench_config:get(riakc_pb_rw, Replies),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),

    %% Choose the node using our ID as a modulus
    TargetIp = lists:nth((Id rem length(Ips)+1), Ips),
    ?INFO("Using target ip ~p for worker ~p\n", [TargetIp, Id]),

    case riakc_pb_socket:start_link(TargetIp, Port) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          r = R,
                          w = W,
                          dw = DW,
                          rw = RW
                         }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p port ~p: ~p\n",
                      [TargetIp, Port, Reason2])
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}]) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(get_existing, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}]) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {error, {not_found, Key}, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
    Robj = riakc_obj:update_value(Robj0, ValueGen()),
    case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                     {dw, State#state.dw}]) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}]) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}]) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
            Robj = riakc_obj:update_value(Robj0, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                             {dw, State#state.dw}]) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}]) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}]) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            {error, {not_found, Key}, State}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:delete(State#state.pid, State#state.bucket, KeyGen(),
                                [{rw, State#state.rw}]) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


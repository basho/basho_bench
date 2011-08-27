%% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_java: Driver for riak java client
%%
%% Copyright (c) 2011 Basho Techonologies
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
-module(basho_bench_driver_riakc_java).

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
    Nodes  = basho_bench_config:get(riakc_java_nodes, [[{'java@127.0.0.1',{127,0,0,1}, 8087}]]),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    Replies = basho_bench_config:get(riakc_java_replies, 2),
    R = basho_bench_config:get(riakc_java_r, Replies),
    W = basho_bench_config:get(riakc_java_w, Replies),
    DW = basho_bench_config:get(riakc_java_dw, Replies),
    RW = basho_bench_config:get(riakc_java_rw, Replies),
    Bucket  = basho_bench_config:get(riakc_java_bucket, <<"test">>),
    Transport = basho_bench_config:get(riakc_java_transport, pb),
    PBBuffer = basho_bench_config:get(riakc_java_pbc_buffer, 16),

    %% Choose the node using our ID as a modulus
    {TargetNode, Ip, Port} = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    %% Check that we can at least talk to the jinterface riak java client node
    case net_adm:ping(TargetNode) of
        pang ->
            ?FAIL_MSG("~s requires that you run a java client jinterface node.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    case basho_bench_java_client:new(TargetNode, Ip, Port, PBBuffer, Transport) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          r = R,
                          w = W,
                          dw = DW,
                          rw = RW
                        }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect java jinterface node ~p on ip ~p to ~p port ~p: ~p\n",
                      [TargetNode, Ip, Port, Reason2])
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case basho_bench_java_client:get(State#state.pid, State#state.bucket, Key, State#state.r) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(get_existing, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case basho_bench_java_client:get(State#state.pid, State#state.bucket, Key, State#state.r) of
        {ok, found} ->
            {ok, State};
        {ok, notfound} ->
            {error, {not_found, Key}, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value =ValueGen(),
    case basho_bench_java_client:put(State#state.pid, State#state.bucket, Key, Value, State#state.w, State#state.dw) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = ValueGen(),
    case basho_bench_java_client:create_update(State#state.pid, State#state.bucket,
                                               Key, Value, State#state.r, State#state.w, State#state.dw) of

        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = ValueGen(),
    case basho_bench_java_client:update(State#state.pid, State#state.bucket,
                                        Key, Value, State#state.r, State#state.w, State#state.dw) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {error, {not_found, Key}, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case basho_bench_java_client:delete(State#state.pid, State#state.bucket, KeyGen(), State#state.rw) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


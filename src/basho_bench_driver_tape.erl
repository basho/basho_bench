%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2014 Basho Techonologies
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
-module(basho_bench_driver_tape).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

%% TODO: consult basho_bench_driver_riakclient.erl

-record(state, { first_post,
                 tape_data,
                 pid,
                 bucket,
                 replies,
                 content_type
         }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    Replies = basho_bench_config:get(riakc_pb_replies, quorum),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),

    Filename = basho_bench_config:get(tape_data, none),
    CT = basho_bench_config:get(riakc_pb_content_type, "application/octet-stream"),
    TapeData =
        case file:consult(Filename) of
            {ok, Contents} ->
                ?INFO("Tape has ~p records\n", [length(Contents)]),
                Contents;
            {error, Reason} ->
                {ok, Cwd} = file:get_cwd(),
                ?FAIL_MSG("Failed to read tape data from ~p: ~p\n",
                          [filename:join([Cwd, Filename]), Reason])
        end,
    TapeDataConverted =
        case CT of
            "application/octet-stream" ->
                [{TS, {term_to_binary(K), term_to_binary(V)}}
                 || {TS, {K, V}} <- TapeData];
            "application/json" ->
                [{TS, {term_to_binary(K), mochijson2:encode(V)}}
                 || {TS, {K, V}} <- TapeData];
            _Unsupported ->
                ?FAIL_MSG("Unsupported content type: ~p\n", [_Unsupported])
        end,

    %% Choose the target node using our ID as a modulus
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),

    case riakc_pb_socket:start_link(TargetIp, TargetPort) of
        {ok, Pid} ->
            {ok, #state { first_post = os:timestamp(),
                          tape_data = TapeDataConverted,
                          pid = Pid,
                          bucket = Bucket,
                          replies = Replies,
                          content_type = CT
                        }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.


run(put, _KeyGen, _ValueGen, #state{tape_data = []}) ->
    {stop, eof};

run(put, _, _, State = #state{first_post = T0,
                              tape_data = [{TS, {Key, Value}} | Rest]}) ->
    Robj = riakc_obj:new(State#state.bucket,
                         Key, Value,
                         State#state.content_type),
    ThisRecordDue = advance_now(T0, TS),
    case timer:now_diff(ThisRecordDue, os:timestamp()) of
        UsecToSleep when UsecToSleep > 0 ->
            timer:sleep(round(UsecToSleep / 1000));
            % apply_after() may blow things up, so sleep() is appropriate here
        _ ->
            we_are_late
    end,
    case riakc_pb_socket:put(State#state.pid, Robj) of
        ok ->
            {ok, State#state{tape_data = Rest}};
        {error, disconnected} ->
            {error, disconnected, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% ====================================================================
%% supporting functions
%% ====================================================================
advance_now({Mega, Sec, Micro}, DeltaUsec) ->
    normalize_now({Mega, Sec, Micro + DeltaUsec}).

normalize_now({_, Sec, Micro} = GoodOne)
  when Sec < 1000000 andalso Micro < 1000000 ->
    GoodOne;
normalize_now({Mega, Sec, Micro})
  when Micro >= 1000000 ->
    normalize_now({Mega, Sec + Micro div 1000000, Micro rem 1000000});
normalize_now({Mega, Sec, Micro})
  when Sec >= 1000000 ->
    normalize_now({Mega + Sec div 1000000, Sec rem 1000000, Micro}).

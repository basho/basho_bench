%% -------------------------------------------------------------------
%%
%% basho_bench_driver_2i_pb: Driver for Secondary Indices (via PB)
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
-module(basho_bench_driver_ts_proto).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {
          conn_ref,
          table,
          series,
          value_size,
          rand_bytes,
          timestamp,
          timeout
         }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Ensure that hackney is started...
    hackney:start(),

    %% Read config settings...
    Host  = basho_bench_config:get(ts_proto_host, <<"127.0.0.1">>),
    Port  = basho_bench_config:get(ts_proto_port, 10018),
    Size = basho_bench_config:get(ts_proto_value_size, 512),
    Timeout = basho_bench_config:get(ts_proto_timeout, 60000),
    Table = iolist_to_binary(io_lib:format("table~p", [Id])),
    Series = iolist_to_binary(io_lib:format("series~p", [Id])),
    RandBytes = base64:encode(crypto:rand_bytes(10 * 1024 * 1024)),
    ?INFO("Using target ~p:~p ~p/~p for worker ~p\n",
          [Host, Port, Table, Series, Id]),

    case hackney:connect(hackney_tcp_transport, Host, Port, []) of
        {ok, ConnRef} ->
            {ok, #state {
                    conn_ref = ConnRef,
                    table = Table,
                    series = Series,
                    rand_bytes = RandBytes,
                    value_size = Size,
                    timestamp = 1,
                    timeout = Timeout
                   }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p port ~p: ~p\n",
                      [Host, Port, Reason2])
    end.

to_input_batch(Table, Series, Time, N, Size, RandBytes) ->
    B0 = <<Table/binary, "\n", Series/binary, "\n">>,
    add_points(N, Size, RandBytes, Time, B0).

add_points(0, _, _, _, B) ->
    B;
add_points(N, ValSize, RandBytes, Time, B) ->
    Ofs = random:uniform(byte_size(RandBytes) - ValSize) - 1,
    <<_:Ofs/binary, V:ValSize/binary, _/binary>> = RandBytes,
    TimeBin = integer_to_binary(Time),
    B2 = <<B/binary, TimeBin/binary, " ", V/binary, "\n">>,
    add_points(N-1, ValSize, RandBytes, Time + 1, B2).

run({put, N}, _KeyGen, _ValueGen, State =
    #state{timestamp = Timestamp, conn_ref = ConnRef,
           table = Table, series = Series, value_size = Size,
           rand_bytes = RandBytes}) ->
    Batch = to_input_batch(Table, Series, Timestamp, N, Size, RandBytes),
    State2 = State#state{timestamp=Timestamp+N},
    case hackney:send_request(ConnRef,
                              {post, <<"/ts/insert">>,
                               [{<<"Content-Type">>, <<"text/plain">>}],
                               Batch}) of
        {ok, _, _, ConnRef} ->
            case hackney:body(ConnRef) of
                {ok, _Body} -> 
                    {ok, State2};
                {error, Error} ->
                    {error, Error, State2}
            end;
        {error, ReqError} ->
            {error, ReqError, State2}
    end;
run({get, N}, _KeyGen, _ValueGen, State =
    #state{timestamp = Timestamp, series = Series,
           conn_ref = ConnRef, table = Table}) ->
    % Adjust number if not enough points yet
    {Start, End} = case Timestamp > N of
                       true ->
                           S = random:uniform(Timestamp - N + 1),
                           {S, S + N};
                       false ->
                           {1, Timestamp}
                   end,
    SBin = integer_to_binary(Start),
    EBin = integer_to_binary(End),
    Query = <<Table/binary, $\n, Series/binary, $\n,
              SBin/binary, $\n, EBin/binary, $\n>>,
    case hackney:send_request(ConnRef,
                              {post, <<"/ts/query">>,
                               [{<<"Content-Type">>, <<"text/plain">>}],
                               Query}) of
        {ok, _, _, ConnRef} ->
            case hackney:body(ConnRef) of
                {ok, _Body} ->
                    {ok, State};
                {error, Error} ->
                    {error, Error, State}
            end;
        {error, ReqError} ->
            {error, ReqError, State}
    end;

run(Other, _, _, _) ->
    throw({unknown_operation, Other}).

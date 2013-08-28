%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2013 Basho Techonologies
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
-module(basho_bench_driver_memcached).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {socket}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    IPs = basho_bench_config:get(memcached_ips, ["127.0.0.1"]),
    DefaultPort = basho_bench_config:get(memcached_default_port, 11211),
    %% TODO: Only first entry is used
    [{IP, Port} | _] = basho_bench_config:normalize_ips(IPs, DefaultPort),
    {ok, Socket} = gen_tcp:connect(IP, Port, [{active, false}, binary, {nodelay, true},
                                              {packet, line}]),
    {ok, #state{socket = Socket}}.

run(get, KeyGen, _ValueGen, #state{socket = Socket} = State) ->
    Key = KeyGen(),
    gen_tcp:send(Socket, [<<"get ">>, Key, <<"\r\n">>]),
    case receive_data(Socket) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(set, KeyGen, ValueGen, #state{socket = Socket} = State) ->
    Key = KeyGen(),
    Value = ValueGen(),
    Bytes = integer_to_list(byte_size(Value)),
    gen_tcp:send(Socket, [<<"set ">>, Key, <<" 0 0 ">>, Bytes, <<"\r\n">>,
                          Value, <<"\r\n">>]),
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            case Data of
                <<"STORED\r\n">> ->
                    {ok, State};
                Error ->
                    {error, Error, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end;
run(incr, KeyGen, _ValueGen, #state{socket = Socket} = State) ->
    Key = KeyGen(),
    gen_tcp:send(Socket, [<<"incr ">>, Key, <<" 1\r\n">>]),
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            ValueSize = byte_size(Data)-2,
            case Data of
                <<"NOT_FOUND\r\n">> ->
                    {error, not_found, State};
                <<Value:ValueSize/binary, "\r\n">> ->
                    case catch list_to_integer(binary_to_list(Value)) of
                        {'EXIT', {badarg, _}} ->
                            {error, Value, State};
                        _ ->
                            {ok, State}
                    end;
                Error ->
                    {error, Error, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

receive_data(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, <<"END\r\n">>} ->
            {error, not_found};
        {ok, Data} ->
            [<<"VALUE">>, _K, _, BytesBin | _] =
                binary:split(Data, [<<" ">>, <<"\r\n">>], [global, trim]),
            inet:setopts(Socket,[{packet, raw}]),
            %% 7 is length of CR+LF + "END" + CR+LF
            case gen_tcp:recv(Socket, list_to_integer(binary_to_list(BytesBin)) + 7) of
                {ok, _} ->
                    inet:setopts(Socket,[{packet, line}]),
                    ok;
                {error, Reason} ->
                    inet:setopts(Socket,[{packet, line}]),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

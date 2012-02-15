%% -------------------------------------------------------------------
%%
%% basho_bench_java_client: Local API to remote Java client
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
-module(basho_bench_java_client).

-export([new/5, get/4, put/6, create_update/7, update/7, delete/4]).

-define(TIMEOUT, 60*1000).

%%% Ask the java node to create a new process, and link to it
new(Node, Ip, Port, PBBuffer, Transport) ->
    erlang:send({factory, Node}, {self(), {Ip, Port, PBBuffer, Transport}}),

    receive
        Pid when is_pid(Pid) ->
            link(Pid),
            {ok, Pid}
    after ?TIMEOUT ->
            {error, timeout}
    end.

get(Pid, Bucket, Key, R) ->
    Pid ! {self(), {get, [{bucket, Bucket}, {key, Key}, {r, R}]}},
    receive
        {Pid, Res} ->
            ok
    end,
    Res.

put(Pid, Bucket, Key, Value, W, DW) ->
    Pid ! {self(), {put, [{bucket, Bucket}, {key, Key}, {value, Value}, {w, W}, {dw, DW}]}},

    receive 
        {Pid, Res} ->
            ok
    end,
    Res.
            
create_update(Pid, Bucket, Key, Value, R, W, DW) ->
    Pid ! {self(), {create_update, [{bucket, Bucket}, {key, Key}, {value, Value},
                                    {r, R}, {w, W}, {dw, DW}]}},
    
    receive
        {Pid, Res} ->
            ok
    end,
    Res.

update(Pid, Bucket, Key, Value, R, W, DW) ->
    Pid ! {self(), {update, [{bucket, Bucket}, {key, Key}, {value, Value},
                                    {r, R}, {w, W}, {dw, DW}]}},
    
    receive
        {Pid, Res} ->
            ok
    end,
    Res.

delete(Pid, Bucket, Key, RW) ->
    Pid ! {self(), {delete, [{bucket, Bucket}, {key, Key}, {r, RW}]}}, %%HACK to reuse GetArgs on java side, change
    receive
        {Pid, Res} ->
            ok
    end,
    Res.


%% -------------------------------------------------------------------
%%
%% riak_bench: Benchmarking Suite for Riak
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
-module(riak_bench_valgen).

-export([new/2,
         dimension/2]).

-include("riak_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new({fixed_bin, Size}, Id) ->
    Source = crypto:rand_bytes(Size),
    MaxOffset = Size - 16,
    fun() -> fixed_bin(Source, MaxOffset, Size, <<>>) end;
new(Other, Id) ->
    ?FAIL_MSG("Unsupported value generator requested: ~p\n", [Other]).

dimension({fixed_bin, Size}, KeyDimension) ->
    Size * KeyDimension;
dimension(Other, _) ->
    ?FAIL_MSG("Unsupported value generator dimension requested: ~p\n", [Other]).



%% ====================================================================
%% Internal Functions
%% ====================================================================
fixed_bin(_Source, _MaxOffset, 0, Acc) ->
    Acc;
fixed_bin(Source, MaxOffset, Size, Acc) ->
    Offset = random:uniform(MaxOffset),
    Step = erlang:max(Size, erlang:min(16, Size - 16)),
    io:format("Size: ~p Step: ~p\n", [Size, Step]),
    <<_:Offset/bytes, Slice:Step/bytes, _Rest/binary>> = Source,
    fixed_bin(Source, MaxOffset, Size - Step, <<Acc/binary, Slice/binary>>).

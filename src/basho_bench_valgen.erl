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
-module(basho_bench_valgen).

-export([new/2,
         dimension/2]).

-include("basho_bench.hrl").

-define(SOURCE_SIZE, 4096).
-define(BLOCK_SIZE, 512).
-define(MAX_OFFSET, ?SOURCE_SIZE - ?BLOCK_SIZE).

%% ====================================================================
%% API
%% ====================================================================

new({fixed_bin, Size}, _Id) ->
    Source = crypto:rand_bytes(?SOURCE_SIZE),
    fun() -> data_block(Source, Size, <<>>) end;
new({exponential_bin, MinSize, Lambda}, _Id) ->
    Source = crypto:rand_bytes(?SOURCE_SIZE),
    fun() -> data_block(Source, MinSize + trunc(1 / stats_rv:exponential(Lambda)), <<>>) end;
new(Other, _Id) ->
    ?FAIL_MSG("Unsupported value generator requested: ~p\n", [Other]).

dimension({fixed_bin, Size}, KeyDimension) ->
    Size * KeyDimension;
dimension(Other, _) ->
    0.0.



%% ====================================================================
%% Internal Functions
%% ====================================================================

data_block(_Source, 0, Acc) ->
    Acc;
data_block(Source, Size, Acc) ->
    Offset = random:uniform(?MAX_OFFSET),
    if
        Size > ?BLOCK_SIZE ->
            Step = ?BLOCK_SIZE;
        true ->
            Step = Size
    end,
    <<_:Offset/bytes, Slice:Step/bytes, _Rest/binary>> = Source,
    data_block(Source, Size - Step, <<Acc/binary, Slice/binary>>).

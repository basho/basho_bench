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

%% ====================================================================
%% API
%% ====================================================================

new({fixed_bin, Size}, _Id) ->
    Source = init_source(),
    fun() -> data_block(Source, Size) end;
new({fixed_bin, Size, Val}, _Id) ->
    Data = list_to_binary(lists:duplicate(Size, Val)),
    fun() -> Data end;
new({fixed_char, Size}, _Id) ->
    fun() -> list_to_binary(lists:map(fun (_) -> random:uniform(95)+31 end, lists:seq(1,Size))) end;
new({exponential_bin, MinSize, Mean}, _Id) ->
    Source = init_source(),
    fun() -> data_block(Source, MinSize + trunc(basho_bench_stats:exponential(1 / Mean))) end;
new({uniform_bin, MinSize, MaxSize}, _Id) ->
    Source = init_source(),
    Diff = MaxSize - MinSize,
    fun() -> data_block(Source, MinSize + random:uniform(Diff)) end;
new({function, Module, Function, Args}, Id) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            erlang:apply(Module, Function, [Id] ++ Args);
        _Error ->
            ?FAIL_MSG("Could not find valgen function: ~p:~p\n", [Module, Function])
    end;
new(Other, _Id) ->
    ?FAIL_MSG("Unsupported value generator requested: ~p\n", [Other]).

dimension({fixed_bin, Size}, KeyDimension) ->
    Size * KeyDimension;
dimension(_Other, _) ->
    0.0.



%% ====================================================================
%% Internal Functions
%% ====================================================================

init_source() ->
    SourceSz = basho_bench_config:get(value_generator_source_size, 1048576),
    {SourceSz, crypto:rand_bytes(SourceSz)}.

data_block({SourceSz, Source}, BlockSize) ->
    case SourceSz - BlockSize > 0 of
        true ->
            Offset = random:uniform(SourceSz - BlockSize),
            <<_:Offset/bytes, Slice:BlockSize/bytes, _Rest/binary>> = Source,
            Slice;
        false ->
            ?WARN("value_generator_source_size is too small; it needs a value > ~p.\n",
                  [BlockSize]),
            Source
    end.

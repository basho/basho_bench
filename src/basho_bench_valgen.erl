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

new({fixed_bin, Size}, Id)
  when is_integer(Size), Size >= 0 ->
    Source = init_source(Id),
    fun() -> data_block(Source, Size) end;
new({fixed_bin, Size, Val}, _Id)
  when is_integer(Size), Size >= 0, is_integer(Val), Val >= 0, Val =< 255 ->
    Data = list_to_binary(lists:duplicate(Size, Val)),
    fun() -> Data end;
new({fixed_char, Size}, _Id)
  when is_integer(Size), Size >= 0 ->
    fun() -> list_to_binary(lists:map(fun (_) -> rand:uniform(95)+31 end, lists:seq(1,Size))) end;
new({exponential_bin, MinSize, Mean}, Id)
  when is_integer(MinSize), MinSize >= 0, is_number(Mean), Mean > 0 ->
    Source = init_source(Id),
    fun() -> data_block(Source, MinSize + trunc(basho_bench_stats:exponential(1 / Mean))) end;
new({uniform_bin, MinSize, MaxSize}, Id) 
  when is_integer(MinSize), is_integer(MaxSize), MinSize < MaxSize ->
    Source = init_source(Id),
    Diff = MaxSize - MinSize,
    fun() -> data_block(Source, MinSize + rand:uniform(Diff)) end;
new({function, Module, Function, Args}, Id)
  when is_atom(Module), is_atom(Function), is_list(Args) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            erlang:apply(Module, Function, [Id] ++ Args);
        _Error ->
            ?FAIL_MSG("Could not find valgen function: ~p:~p\n", [Module, Function])
    end;
new({uniform_int, MaxVal}, _Id)
  when is_integer(MaxVal), MaxVal >= 1 ->
    fun() -> rand:uniform(MaxVal) end;
new({uniform_int, MinVal, MaxVal}, _Id)
  when is_integer(MinVal), is_integer(MaxVal), MaxVal > MinVal ->
    fun() -> rand:uniform(MaxVal - MinVal) + MinVal end;
new({semi_compressible, MinSize, Mean, XLMult, XLProb}, Id)
    when is_integer(MinSize), MinSize >= 0, is_number(Mean), Mean > 0 ->
    Source = init_altsource(Id),
    fun() ->
        R = rand:uniform(),
        {ModMin, ModMean} = 
            case R < XLProb of
                true ->
                    {XLMult * MinSize, XLMult * Mean};
                false ->
                    {MinSize, Mean}
            end,
        data_block(Source,
                    ModMin + trunc(basho_bench_stats:exponential(1 / ModMean)))
    end;
new(Other, _Id) ->
    ?FAIL_MSG("Invalid value generator requested: ~p\n", [Other]).

dimension({fixed_bin, Size}, KeyDimension) ->
    Size * KeyDimension;
dimension(_Other, _) ->
    0.0.



%% ====================================================================
%% Internal Functions
%% ====================================================================

-define(TAB, valgen_bin_tab).

init_source(Id) ->
    init_source(Id, basho_bench_config:get(?VAL_GEN_BLOB_CFG, undefined)).

init_source(1, undefined) ->
    SourceSz = basho_bench_config:get(?VAL_GEN_SRC_SIZE, 96*1048576),
    ?INFO("Random source: calling crypto:strong_rand_bytes(~w) (override with the '~w' config option\n", [SourceSz, ?VAL_GEN_SRC_SIZE]),
    Bytes = crypto:strong_rand_bytes(SourceSz),
    try
        ?TAB = ets:new(?TAB, [public, named_table]),
        true = ets:insert(?TAB, {x, Bytes})
    catch _:_ -> rerunning_id_1_init_source_table_already_exists
    end,
    ?INFO("Random source: finished crypto:strong_rand_bytes(~w)\n", [SourceSz]),
    {?VAL_GEN_SRC_SIZE, SourceSz, Bytes};
init_source(_Id, undefined) ->
    [{_, Bytes}] = ets:lookup(?TAB, x),
    {?VAL_GEN_SRC_SIZE, size(Bytes), Bytes};
init_source(Id, Path) ->
    {Path, {ok, Bin}} = {Path, file:read_file(Path)},
    if Id == 1 -> ?DEBUG("path source ~p ~p\n", [size(Bin), Path]);
       true    -> ok
    end,
    {?VAL_GEN_BLOB_CFG, size(Bin), Bin}.

init_altsource(Id) ->
    init_altsource(Id, basho_bench_config:get(?VAL_GEN_BLOB_CFG, undefined)).

init_altsource(1, undefined) ->
    GenRandStrFun = fun(_X) -> rand:uniform(95) + 31 end,
    RandomStrs =
        lists:map(fun(X) ->
                        SL = lists:map(GenRandStrFun, lists:seq(1, 128)),
                        {X, list_to_binary(SL)}
                    end,
                    lists:seq(1, 16)),
    ComboBlockFun =
        fun(_X, Acc) ->
            Bin1 = crypto:strong_rand_bytes(4096),
            Bin2 = create_random_textblock(32, RandomStrs),
            % Both the compressible and uncompressible parts will be 
            % 4096 bytes in size.  zlib will compress the compressible
            % part down 3:1
            <<Acc/binary, Bin1/binary, Bin2/binary>>
        end,
    Bytes = lists:foldl(ComboBlockFun, <<>>, lists:seq(1, 8192)),
    SourceSz = byte_size(Bytes),
    try
        ?TAB = ets:new(?TAB, [public, named_table]),
        true = ets:insert(?TAB, {x, Bytes})
    catch _:_ -> rerunning_id_1_init_source_table_already_exists
    end,
    ?INFO("Finished generating random source size (~w)\n", [SourceSz]),
    {?VAL_GEN_SRC_SIZE, SourceSz, Bytes};
init_altsource(_Id, undefined) ->
    [{_, Bytes}] = ets:lookup(?TAB, x),
    {?VAL_GEN_SRC_SIZE, size(Bytes), Bytes};
init_altsource(Id, Path) ->
    {Path, {ok, Bin}} = {Path, file:read_file(Path)},
    if Id == 1 -> ?DEBUG("path source ~p ~p\n", [size(Bin), Path]);
       true    -> ok
    end,
    {?VAL_GEN_BLOB_CFG, size(Bin), Bin}.

create_random_textblock(BlockLength, RandomStrs) ->
    GetRandomBlockFun =
        fun(X, Acc) ->
            Rand = rand:uniform(min(X, 16)),
            {Rand, Block} = lists:keyfind(Rand, 1, RandomStrs),
            <<Acc/binary, Block/binary>>
        end,
    lists:foldl(GetRandomBlockFun, <<>>, lists:seq(1, BlockLength)).

data_block({SourceCfg, SourceSz, Source}, BlockSize) ->
    case SourceSz - BlockSize > 0 of
        true ->
            Offset = rand:uniform(SourceSz - BlockSize),
            <<_:Offset/bytes, Slice:BlockSize/bytes, _Rest/binary>> = Source,
            Slice;
        false ->
            ?WARN("~p is too small ~p < ~p\n",
                  [SourceCfg, SourceSz, BlockSize]),
            Source
    end.
    


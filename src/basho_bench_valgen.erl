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

-compile(export_all).
-include("basho_bench.hrl").

%% Local machines tended to bug out with Lambdas > 700.
%% Can be modified, depending on where it's running.
-define(LAMBDA_APPROX_NORM_THRESHOLD, 700).

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
%% Create a set of binaries with elements of Size and a cardinality of Card
new({fixed_bin_set, Size, Card}, Id) when is_integer(Size), Size >= 0 ->
    basho_bench_config:set(?VAL_GEN_SRC_SIZE, Size*Card),
    Source = init_source(Id),
    fun() -> data_block(fun aligned_offset/2, Source, Size) end;
new({var_bin_set, Lambda, Card, poisson}, Id) when Lambda > 0 ->
    DistSet = [poisson(Lambda) || _ <- lists:seq(1, Card)],
    MaxSize = lists:max(DistSet),
    new({var_bin_set, DistSet, MaxSize, Card}, Id);
new({var_bin_set, Lambda, LambdaThresh, Card, poisson}, Id) when Lambda > 0 ->
    DistSet = [poisson(Lambda, LambdaThresh) || _ <- lists:seq(1, Card)],
    MaxSize = lists:max(DistSet),
    new({var_bin_set, DistSet, MaxSize, Card}, Id);
new({var_bin_set, MinSize, Mean, Card, exponential}, Id)
  when is_integer(MinSize), is_integer(Mean), Mean > MinSize ->
    DistSet = [exponential(MinSize, Mean) || _ <- lists:seq(1, Card)],
    MaxSize = lists:max(DistSet),
    new({var_bin_set, DistSet, MaxSize, Card}, Id);
new({var_bin_set, DistSet, MaxSize, _Card}=ValGenInfo, Id)
  when is_list(DistSet), is_integer(MaxSize) ->
    var_bin_gen(ValGenInfo, Id);
new({var_bin_set, MinSize, MaxSize, _Card}=ValGenInfo, Id)
  when is_integer(MinSize), is_integer(MaxSize), MinSize >= 0, MinSize =< MaxSize ->
    var_bin_gen(ValGenInfo, Id);
new({fixed_char, Size}, _Id)
  when is_integer(Size), Size >= 0 ->
    fun() -> list_to_binary(lists:map(fun (_) -> random:uniform(95)+31 end, lists:seq(1,Size))) end;
new({exponential_bin, MinSize, Mean}, Id)
  when is_integer(MinSize), MinSize >= 0, is_number(Mean), Mean > 0 ->
    Source = init_source(Id),
    fun() -> data_block(Source, exponential(MinSize, Mean)) end;
new({uniform_bin, MinSize, MaxSize}, Id)
  when is_integer(MinSize), is_integer(MaxSize), MinSize < MaxSize ->
    Source = init_source(Id),
    Diff = MaxSize - MinSize,
    fun() -> data_block(Source, MinSize + random:uniform(Diff)) end;
new({poisson_bin, Lambda}, Id)
  when is_integer(Lambda), Lambda > 0 ->
    Source = init_source(Id),
    fun() -> data_block(Source, poisson(Lambda)) end;
new({poisson_bin, Lambda, LambdaThresh}, Id)
  when is_integer(Lambda), Lambda > 0, LambdaThresh >= Lambda ->
    Source = init_source(Id),
    fun() -> data_block(Source, poisson(Lambda, LambdaThresh)) end;
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
    fun() -> random:uniform(MaxVal) end;
new({uniform_int, MinVal, MaxVal}, _Id)
  when is_integer(MinVal), is_integer(MaxVal), MaxVal > MinVal ->
    fun() -> crypto:rand_uniform(MinVal, MaxVal+1) end;
new({bin_seed, Size}, Id) when is_integer(Size), Size >= 0 ->
    basho_bench_config:set(?VAL_GEN_SRC_SIZE, Size),
    {_, _, Bytes} = init_source(Id),
    fun() -> Bytes end;
new({keygen, KeyGen}, Id) ->
    %% Use a KeyGen as a Value generator
    basho_bench_keygen:new(KeyGen, Id);
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
-define(BINS, valgen_var_bins).

init_var_bins(1, {SizeOrSet, MaxSize, Source}) ->
    VarBins = split_bin_blocks(SizeOrSet, MaxSize, Source),
    try
        ?BINS = ets:new(?BINS, [public, named_table]),
        true = ets:insert(?BINS, {x, VarBins})
    catch _:_ -> rerunning_id_1_init_var_bins_table_already_exists
    end,
    VarBins;
init_var_bins(_, _) ->
    [{_, VarBins}] = ets:lookup(?BINS, x),
    VarBins.

init_source(Id) ->
    init_source(Id, basho_bench_config:get(?VAL_GEN_BLOB_CFG, undefined)).

init_source(1, undefined) ->
    SourceSz = basho_bench_config:get(?VAL_GEN_SRC_SIZE, 96*1048576),
    ?INFO("Random source: calling crypto:rand_bytes(~w) (override with the '~w' config option\n", [SourceSz, ?VAL_GEN_SRC_SIZE]),
    Bytes = crypto:rand_bytes(SourceSz),
    try
        ?TAB = ets:new(?TAB, [public, named_table]),
        true = ets:insert(?TAB, {x, Bytes})
    catch _:_ -> rerunning_id_1_init_source_table_already_exists
    end,
    ?INFO("Random source: finished crypto:rand_bytes(~w)\n", [SourceSz]),
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

data_block(Source, BlockSize) ->
    data_block(fun random_offset/2, Source, BlockSize).
data_block(OffsetFun, {SourceCfg, SourceSz, Source}, BlockSize) ->
    case SourceSz - BlockSize > 0 of
        true ->
            Offset = OffsetFun(SourceSz, BlockSize),
            <<_:Offset/bytes, Slice:BlockSize/bytes, _Rest/binary>> = Source,
            Slice;
        false ->
            ?WARN("~p is too small ~p < ~p\n",
                  [SourceCfg, SourceSz, BlockSize]),
            Source
    end.

random_offset(SourceSz, BlockSize) ->
    random:uniform(SourceSz - BlockSize).

aligned_offset(SourceSz, BlockSize) ->
    (random:uniform(SourceSz - BlockSize) div BlockSize) * BlockSize.

var_bin_gen({_, SizeOrSet, MaxSize, Card}, Id) ->
    basho_bench_config:set(?VAL_GEN_SRC_SIZE, MaxSize*Card),
    Source = init_source(Id),
    VarBins = init_var_bins(Id, {SizeOrSet, MaxSize, Source}),
    %% Store list of varbins in ets for concurrent workers
    fun() -> lists:nth(random:uniform(Card), VarBins) end.

split_bin_blocks(SizeOrSet, MaxSize, {_SourceCfg, _SourceSz, Source}) ->
    split_bin_blocks(SizeOrSet, MaxSize, Source, []).

split_bin_blocks([], _MaxSize, _Source, Acc) ->
    Acc;
split_bin_blocks([H|T], MaxSize, Source, Acc) ->
    Size = H,
    split_bin_blocks(T, Size, MaxSize, Source, Acc);

split_bin_blocks(MinSize, MaxSize, Source, Acc) when is_integer(MinSize) ->
    Size = crypto:rand_uniform(MinSize, MaxSize + 1),
    split_bin_blocks(MinSize, Size, MaxSize, Source, Acc).
split_bin_blocks(X, Size, MaxSize, Source, Acc) ->
        Padding = MaxSize - Size,
    case Source of
        <<>> ->
            Acc;
        <<Chunk:Size/bytes, _:Padding/bytes, Rest/binary>> ->
            split_bin_blocks(X, MaxSize, Rest, [Chunk|Acc]);
        Bin ->
            [Bin|Acc]
    end.

poisson(Lambda) ->
    poisson(Lambda, ?LAMBDA_APPROX_NORM_THRESHOLD).
poisson(Lambda, ApproxNormThreshold) ->
    %% Since this can be very slow, approximate a normal dist. if greater
    %% than the treshold
    case Lambda > ApproxNormThreshold of
        true -> trunc(normal(Lambda, math:sqrt(Lambda)));
        false ->
            P = math:exp(-Lambda),
            poisson(0, Lambda, P, P, random:uniform())
    end.

poisson(X0, Lambda, P0, S, U)
 when U > S ->
   X1 = X0 + 1,
   P1 = P0 * Lambda / X1,
   poisson(X1, Lambda, P1, S + P1, U);
poisson(X, _, _, _, _) ->
   X.

exponential(MinSize, Mean) ->
    MinSize + trunc(basho_bench_stats:exponential(1 / Mean)).

%% @see https://github.com/basho/basho_stats/blob/develop/src/basho_stats_rv.erl#L54
normal(Mean, Sigma) ->
    Rv1 = random:uniform(),
    Rv2 = random:uniform(),
    Rho = math:sqrt(-2 * math:log(1-Rv2)),
    Rho * math:cos(2 * math:pi() * Rv1) * Sigma + Mean.

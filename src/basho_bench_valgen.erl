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
    GenRandLCFun = fun(_X) -> rand:uniform(26) + 96 end,
    RandomStrGen =
        fun(GenFun) ->
            lists:map(fun(X) ->
                            SL = lists:map(GenFun, lists:seq(1, 128)),
                            {X, list_to_binary(SL)}
                        end,
                        lists:seq(1, 16))
        end,
    RandomAscii = RandomStrGen(GenRandStrFun),
    RandomLC = RandomStrGen(GenRandLCFun),

    ComboBlockFun =
        fun(X, Acc) ->
            Bin1 = crypto:strong_rand_bytes(1024),
            Bin2 = create_random_textblock(8, RandomAscii),
            LI = lorem_ipsum(),
            LIN = (X rem length(LI)) + 1,
            Bin3 = lists:nth(LIN, LI),
            Bin4 = create_random_textblock(8, RandomLC),

            % Both the compressible and uncompressible parts will be 
            % 4096 bytes in size.  zlib will compress the compressible
            % part down 3:1
            <<Acc/binary, Bin1/binary, Bin2/binary, Bin3/binary, Bin4/binary>>
        end,
    Bytes = lists:foldl(ComboBlockFun, <<>>, lists:seq(1, 16384)),
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
    

lorem_ipsum() ->
    [<<"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
            Porttitor massa id neque aliquam vestibulum morbi blandit cursus risus. Leo in vitae turpis massa sed elementum tempus. 
            Est sit amet facilisis magna etiam tempor orci. Scelerisque felis imperdiet proin fermentum. Euismod in pellentesque massa placerat duis ultricies lacus. 
            Sociis natoque penatibus et magnis dis parturient montes nascetur. Duis at consectetur lorem donec massa sapien. Sit amet nisl purus in mollis nunc sed. 
            Sed viverra tellus in hac habitasse platea. Orci ac auctor augue mauris augue neque. Facilisis leo vel fringilla est ullamcorper. 
            Mauris sit amet massa vitae tortor condimentum lacinia quis vel. Magna ac placerat vestibulum lectus mauris.">>,
        <<"Dictum fusce ut placerat orci nulla pellentesque dignissim. Rutrum quisque non tellus orci ac auctor augue mauris augue. 
            Erat imperdiet sed euismod nisi. Et ultrices neque ornare aenean. Mauris pellentesque pulvinar pellentesque habitant morbi tristique senectus et netus. 
            Dolor sit amet consectetur adipiscing elit ut aliquam purus sit. Consectetur adipiscing elit pellentesque habitant morbi tristique. 
            Purus sit amet volutpat consequat mauris. Maecenas accumsan lacus vel facilisis. Lectus arcu bibendum at varius vel pharetra vel turpis nunc. 
            Id semper risus in hendrerit gravida rutrum. Nisl rhoncus mattis rhoncus urna neque viverra justo. Urna cursus eget nunc scelerisque viverra mauris. 
            Sit amet massa vitae tortor condimentum lacinia quis vel. Consectetur adipiscing elit ut aliquam purus. 
            In pellentesque massa placerat duis ultricies lacus sed. Dignissim cras tincidunt lobortis feugiat vivamus at augue eget.">>,
        <<"Adipiscing at in tellus integer feugiat. Quam quisque id diam vel quam. Elementum integer enim neque volutpat ac tincidunt vitae semper. 
            Sit amet mauris commodo quis. Amet facilisis magna etiam tempor orci eu lobortis. Sit amet luctus venenatis lectus magna fringilla urna porttitor. 
            Fringilla urna porttitor rhoncus dolor purus non enim praesent. Nulla pellentesque dignissim enim sit amet venenatis urna cursus eget. 
            Dictum at tempor commodo ullamcorper a lacus. Lacus sed turpis tincidunt id aliquet risus. Turpis massa sed elementum tempus. 
            Eu scelerisque felis imperdiet proin fermentum leo vel orci. Quam id leo in vitae turpis massa sed. 
            Lectus urna duis convallis convallis tellus id interdum velit. In hac habitasse platea dictumst. 
            Mattis enim ut tellus elementum sagittis. Aliquam sem et tortor consequat. Elementum tempus egestas sed sed risus pretium. 
            Nullam ac tortor vitae purus faucibus ornare suspendisse sed.">>,
        <<"Faucibus et molestie ac feugiat sed lectus. Sit amet facilisis magna etiam tempor. Quam vulputate dignissim suspendisse in. 
            Diam ut venenatis tellus in metus vulputate eu. Volutpat odio facilisis mauris sit amet. Interdum varius sit amet mattis vulputate enim nulla. 
            Adipiscing commodo elit at imperdiet dui. Placerat duis ultricies lacus sed turpis tincidunt. Pharetra massa massa ultricies mi quis hendrerit dolor magna eget. 
            Sollicitudin aliquam ultrices sagittis orci a scelerisque. Et odio pellentesque diam volutpat commodo sed egestas egestas. 
            Orci sagittis eu volutpat odio facilisis. Massa tempor nec feugiat nisl pretium fusce id velit ut. Imperdiet dui accumsan sit amet nulla facilisi morbi. 
            Volutpat diam ut venenatis tellus in metus vulputate eu. Arcu ac tortor dignissim convallis.">>,
        <<"Vestibulum lectus mauris ultrices eros in cursus turpis massa. Mus mauris vitae ultricies leo integer malesuada nunc vel risus. 
            Ullamcorper eget nulla facilisi etiam dignissim diam quis enim. Amet dictum sit amet justo donec enim diam vulputate. 
            Duis tristique sollicitudin nibh sit amet commodo nulla facilisi nullam. Diam volutpat commodo sed egestas egestas fringilla phasellus faucibus. 
            Malesuada bibendum arcu vitae elementum curabitur. Sit amet risus nullam eget felis eget nunc lobortis. Est ante in nibh mauris cursus mattis molestie a iaculis. 
            Tristique sollicitudin nibh sit amet. Sagittis nisl rhoncus mattis rhoncus urna neque viverra.">>,
        <<"Senectus et netus et malesuada fames. Nunc aliquet bibendum enim facilisis gravida neque. Pretium fusce id velit ut tortor pretium viverra suspendisse potenti. 
            Ornare lectus sit amet est placerat in egestas erat imperdiet. Tempus quam pellentesque nec nam aliquam sem et tortor. 
            Est ullamcorper eget nulla facilisi etiam dignissim diam quis. Blandit cursus risus at ultrices mi tempus imperdiet. 
            Amet nisl purus in mollis nunc sed id. Ac tortor dignissim convallis aenean. Nulla facilisi etiam dignissim diam quis enim lobortis scelerisque fermentum. 
            Blandit aliquam etiam erat velit scelerisque in.">>,
        <<"Duis tristique sollicitudin nibh sit amet commodo nulla. Neque vitae tempus quam pellentesque nec. In ante metus dictum at tempor. 
            Egestas pretium aenean pharetra magna. Purus in massa tempor nec feugiat nisl pretium. Sapien eget mi proin sed libero enim. 
            Vitae ultricies leo integer malesuada nunc vel risus commodo viverra. Felis eget nunc lobortis mattis aliquam faucibus purus. 
            Cursus metus aliquam eleifend mi. Volutpat diam ut venenatis tellus in metus vulputate eu. 
            Lorem ipsum dolor sit amet consectetur adipiscing elit duis tristique. Tortor consequat id porta nibh venenatis cras sed felis.">>,
        <<"Diam vel quam elementum pulvinar etiam non. Nunc sed blandit libero volutpat sed cras ornare arcu dui. 
            Nisl condimentum id venenatis a condimentum vitae sapien pellentesque habitant. Laoreet sit amet cursus sit. Lorem mollis aliquam ut porttitor. 
            Fames ac turpis egestas integer eget aliquet nibh. Risus in hendrerit gravida rutrum quisque non. 
            Elit scelerisque mauris pellentesque pulvinar pellentesque habitant morbi. Fermentum iaculis eu non diam phasellus. 
            Pretium nibh ipsum consequat nisl vel pretium lectus quam id. Hac habitasse platea dictumst quisque sagittis purus sit amet. 
            Maecenas accumsan lacus vel facilisis volutpat est. Praesent elementum facilisis leo vel. Id cursus metus aliquam eleifend mi in nulla. 
            Odio facilisis mauris sit amet massa. Nunc mi ipsum faucibus vitae aliquet nec ullamcorper sit amet. 
            Amet nisl suscipit adipiscing bibendum est. Vehicula ipsum a arcu cursus.">>,
        <<"Nisi scelerisque eu ultrices vitae. In hac habitasse platea dictumst quisque sagittis purus sit. 
            Arcu dui vivamus arcu felis bibendum ut tristique. Ullamcorper malesuada proin libero nunc. 
            Id diam maecenas ultricies mi eget mauris pharetra et ultrices. Eget nunc lobortis mattis aliquam faucibus purus in. 
            Turpis egestas integer eget aliquet nibh praesent. Nunc faucibus a pellentesque sit amet. Sem fringilla ut morbi tincidunt augue interdum velit euismod. 
            Tellus mauris a diam maecenas sed enim ut. Mattis pellentesque id nibh tortor id aliquet. Mauris commodo quis imperdiet massa. 
            Viverra mauris in aliquam sem fringilla ut. Elementum curabitur vitae nunc sed velit dignissim sodales ut eu. 
            Gravida cum sociis natoque penatibus et magnis dis parturient. Faucibus purus in massa tempor nec feugiat nisl. 
            Tellus cras adipiscing enim eu turpis egestas pretium aenean. Ultrices sagittis orci a scelerisque purus semper eget.">>,
        <<"Platea dictumst quisque sagittis purus sit amet volutpat. Pellentesque massa placerat duis ultricies lacus sed. 
            Sit amet luctus venenatis lectus magna fringilla urna. Massa tincidunt dui ut ornare lectus sit amet est placerat. 
            A condimentum vitae sapien pellentesque habitant morbi. Nisl purus in mollis nunc sed id semper risus in. 
            Gravida cum sociis natoque penatibus et magnis dis. Nec tincidunt praesent semper feugiat. Vel orci porta non pulvinar neque laoreet. 
            Sed viverra tellus in hac habitasse platea dictumst vestibulum.">>].


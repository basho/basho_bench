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
-module(basho_bench_keygen).

-export([new/2,
         dimension/1,
         sequential_int_generator/4]).
-export([reset_sequential_int_state/0]).        % Internal driver use only.

-include("basho_bench.hrl").

%% Use a fixed shape for Pareto that will yield the desired 80/20
%% ratio of generated values.
-define(PARETO_SHAPE, 1.5).

%% ====================================================================
%% API
%% ====================================================================
new({int_to_bin, InputGen}, Id) ->
    ?WARN("The int_to_bin key generator wrapper is deprecated, please use the "
          "int_to_bin_bigendian or int_to_bin_littleendian wrapper instead\n",
          []),
    Gen = new(InputGen, Id),
    fun() -> <<(Gen()):32/native>> end;
new({int_to_bin_bigendian, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> <<(Gen()):32/big>> end;
new({int_to_bin_littleendian, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> <<(Gen()):32/little>> end;
new({int_to_str, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> integer_to_list(Gen()) end;
new({to_binstr, FmtStr, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> list_to_binary(io_lib:format(FmtStr, [Gen()])) end;
new({base64, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> base64:encode(Gen()) end;
new({concat_binary, OneGen, TwoGen}, Id) ->
    Gen1 = new(OneGen, Id),
    Gen2 = new(TwoGen, Id),
    fun() ->
            <<(Gen1())/binary, (Gen2())/binary>>
    end;
new({sequential_int, MaxKey}, Id) ->
    Ref = make_ref(),
    DisableProgress =
        basho_bench_config:get(disable_sequential_int_progress_report, false),
    fun() -> sequential_int_generator(Ref, MaxKey, Id, DisableProgress) end;
new({partitioned_sequential_int, MaxKey}, Id) ->
    new({partitioned_sequential_int, 0, MaxKey}, Id);
new({partitioned_sequential_int, StartKey, NumKeys}, Id) ->
    Workers = basho_bench_config:get(concurrent),
    Range = NumKeys div Workers,
    MinValue = StartKey + Range * (Id - 1),
    MaxValue = StartKey +
               % Last worker picks up remainder to include entire range
               case Workers == Id of true-> NumKeys; false -> Range * Id end,
    Ref = make_ref(),
    DisableProgress =
        basho_bench_config:get(disable_sequential_int_progress_report, false),
    ?DEBUG("ID ~p generating range ~p to ~p\n", [Id, MinValue, MaxValue]),
    fun() -> sequential_int_generator(Ref, MaxValue - MinValue, Id, DisableProgress) + MinValue end;
new({uniform_int, MaxKey}, _Id) ->
    fun() -> random:uniform(MaxKey) end;
new({uniform_int, StartKey, NumKeys}, _Id) ->
    fun() -> random:uniform(NumKeys) + StartKey - 1 end;
new({pareto_int, MaxKey}, _Id) ->
    pareto(trunc(MaxKey * 0.2), ?PARETO_SHAPE);
new({truncated_pareto_int, MaxKey}, Id) ->
    Pareto = new({pareto_int, MaxKey}, Id),
    fun() -> erlang:min(MaxKey, Pareto()) end;
new(uuid_v4, _Id) ->
    fun() -> uuid:v4() end;
new({function, Module, Function, Args}, Id) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            erlang:apply(Module, Function, [Id] ++ Args);
        _Error ->
            ?FAIL_MSG("Could not find keygen function: ~p:~p\n", [Module, Function])
    end;
%% Adapt a value generator. The function keygen would work if Id was added as 
%% the last parameter. But, alas, it is added as the first.
new({valgen, ValGen}, Id) ->
    basho_bench_valgen:new(ValGen, Id);
new(Bin, _Id) when is_binary(Bin) ->
    fun() -> Bin end;
new(Other, _Id) ->
    ?FAIL_MSG("Unsupported key generator requested: ~p\n", [Other]).

dimension({int_to_str, InputGen}) ->
    dimension(InputGen);
dimension({int_to_bin, InputGen}) ->
    dimension(InputGen);
dimension({to_binstr, _FmtStr, InputGen}) ->
    dimension(InputGen);
dimension({base64, InputGen}) ->
    dimension(InputGen);
dimension({concat_binary, OneGen, TwoGen}) ->
    erlang:min(dimension(OneGen), dimension(TwoGen));
dimension({sequential_int, MaxKey}) ->
    MaxKey;
dimension({partitioned_sequential_int, MaxKey}) ->
    MaxKey;
dimension({uniform_int, MaxKey}) ->
    MaxKey;
dimension({truncated_pareto_int, MaxKey}) ->
    MaxKey;
dimension(Bin) when is_binary(Bin) ->
    undefined;
dimension(Other) ->
    ?INFO("No dimension available for key generator: ~p\n", [Other]),
    undefined.




%% ====================================================================
%% Internal functions
%% ====================================================================

pareto(Mean, Shape) ->
    S1 = (-1 / Shape),
    S2 = Mean * (Shape - 1),
    fun() ->
            U = 1 - random:uniform(),
            trunc((math:pow(U, S1) - 1) * S2)
    end.


sequential_int_generator(Ref, MaxValue, Id, DisableProgress) ->
   %% A bit of evil here. We want to generate numbers in sequence and stop
   %% at MaxKey. This means we need state in our anonymous function. Use the process
   %% dictionary to keep track of where we are.
   case erlang:get({sigen, Ref}) of
       undefined ->
           seq_gen_put(Ref, seq_gen_read_resume_value(Id, MaxValue)),
           sequential_int_generator(Ref, MaxValue, Id, DisableProgress);
       MaxValue ->
           throw({stop, empty_keygen});
       Value ->
           case Value rem 500 of
               400 -> ok = seq_gen_write_resume_value(Id, Value);
                 _ -> ok
           end,
           case (not DisableProgress) andalso Value rem 5000 == 0 of
               true ->
                   Me = self(),
                   spawn(fun() -> ?DEBUG("sequential_int_gen: ~p: ~p (~w%)\n", [Me, Value, trunc(100 * (Value / MaxValue))]) end);
               false ->
                   ok
           end,
           seq_gen_put(Ref, Value+1),
           Value
   end.

seq_gen_put(Ref, Value) ->
    erlang:put({sigen, Ref}, Value).

seq_gen_write_resume_value(Id, Value) ->
    case seq_gen_state_dir(Id) of
        "" ->
            ok;
        Path ->
            OutFile = Path ++ "/" ++ integer_to_list(Id),
            OutFileTmp = Path ++ "/" ++ integer_to_list(Id) ++ ".tmp",
            Bin = term_to_binary(Value),
            ok = file:write_file(OutFileTmp, Bin),
            {ok, Bin} = file:read_file(OutFileTmp),
            ok = file:rename(OutFileTmp, OutFile)
    end.

seq_gen_read_resume_value(Id, MaxValue) ->
    case seq_gen_state_dir(Id) of
        "" ->
            0;
        Path ->
            try
                InFile = Path ++ "/" ++ integer_to_list(Id),
                {ok, Bin} = file:read_file(InFile),
                Value = binary_to_term(Bin),
            case Value > MaxValue of
               true ->
                  ?WARN("Id ~p resume value ~p exceeds maximum value ~p. Restarting from 0",[Id, Value, MaxValue]),
                  0;
               false ->
                  ?DEBUG("Id ~p resuming from value ~p\n", [Id, Value]),
                  Value
            end
            catch
                error:{badmatch, _} ->
                    0;
                X:Y ->
                    ?DEBUG("Error reading resume value for Id ~p ~p: ~p ~p\n",
                           [Id, Path ++ "/" ++ integer_to_list(Id), X, Y]),
                    0
            end
    end.

seq_gen_state_dir(Id) ->
    Key = sequential_int_state_dir,
    DirValid = get(seq_dir_test_res),
    case {basho_bench_config:get(Key, "") , DirValid} of
        {_Dir, false} ->
            "";
        {[$/|_] = Dir, true} ->
            Dir;
        {[$/|_] = Dir, undefined} ->
            case filelib:ensure_dir(filename:join(Dir, "touch")) of
                ok ->
                    put(seq_dir_test_res, true),
                    Dir;
                MkDirErr ->
                    ?WARN("Could not ensure ~p -> ~p was a writable dir: ~p", [Key, Dir, MkDirErr]),
                    put(seq_dir_test_res, false),
                    put(you_have_been_warned, true),
                    ""
            end;
        {Else, _} ->
            case Else /= "" andalso
                 get(you_have_been_warned) == undefined andalso Id == 1 of
                true ->
                    ?WARN("Config value ~p -> ~p is not an absolute "
                          "path, ignoring!\n", [Key, Else]),
                    put(you_have_been_warned, true);
                false ->
                    ok
            end,
            ""
    end.

reset_sequential_int_state() ->
    case [X || {{sigen, X}, _} <- element(2, process_info(self(),
                                                          dictionary))] of
        [Ref] ->
            erlang:put({sigen, Ref}, 0);
        [] ->
            ok
    end.

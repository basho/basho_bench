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
         sequential_int_generator/3]).

-include("basho_bench.hrl").

%% Use a fixed shape for Pareto that will yield the desired 80/20
%% ratio of generated values.
-define(PARETO_SHAPE, 1.5).

%% ====================================================================
%% API
%% ====================================================================
new({int_to_bin, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> <<(Gen()):32/native>> end;
new({int_to_str, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> integer_to_list(Gen()) end;
new({to_binstr, FmtStr, InputGen}, Id) ->
    Gen = new(InputGen, Id),
    fun() -> list_to_binary(io_lib:format(FmtStr, [Gen()])) end;
new({sequential_int, MaxKey}, Id) ->
    Ref = make_ref(),
    fun() -> sequential_int_generator(Ref, MaxKey, Id) end;
new({partitioned_sequential_int, MaxKey}, Id) ->
    new({partitioned_sequential_int, 0, MaxKey}, Id);
new({partitioned_sequential_int, StartKey, NumKeys}, Id) ->
    Workers = basho_bench_config:get(concurrent),
    Range = NumKeys div Workers,
    MinValue = StartKey + Range * (Id - 1),
    MaxValue = StartKey + Range * Id,
    Ref = make_ref(),
    ?DEBUG("ID ~p generating range ~p to ~p\n", [Id, MinValue, MaxValue]),
    fun() -> sequential_int_generator(Ref, Range, Id) + MinValue end;
new({uniform_int, MaxKey}, _Id) ->
    fun() -> random:uniform(MaxKey) end;
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
new(Other, _Id) ->
    ?FAIL_MSG("Unsupported key generator requested: ~p\n", [Other]).

dimension({Converter, InputGen}) when Converter == int_to_str orelse Converter == int_to_bin ->
    dimension(InputGen);
dimension({sequential_int, MaxKey}) ->
    MaxKey;
dimension({partitioned_sequential_int, MaxKey}) ->
    MaxKey;
dimension({uniform_int, MaxKey}) ->
    MaxKey;
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


sequential_int_generator(Ref, MaxValue, Id) ->
   %% A bit of evil here. We want to generate numbers in sequence and stop
   %% at MaxKey. This means we need state in our anonymous function. Use the process
   %% dictionary to keep track of where we are.
   case erlang:get({sigen, Ref}) of
       undefined ->
           seq_gen_put(Ref, seq_gen_read_resume_value(Id)),
           sequential_int_generator(Ref, MaxValue, Id);
       MaxValue ->
           throw({stop, empty_keygen});
       Value ->
           case Value rem 500 of
               0 -> ok = seq_gen_write_resume_value(Id, Value);
               _ -> ok
           end,
           case Value rem 5000 of
               0 ->
                   ?DEBUG("sequential_int_gen: ~p (~w%)\n", [Value, trunc(100 * (Value / MaxValue))]);
               _ ->
                   ok
           end,
           seq_gen_put(Ref, Value+1),
           Value
   end.

seq_gen_put(Ref, Value) ->
    erlang:put({sigen, Ref}, Value).

seq_gen_write_resume_value(Id, Value) ->
    case seq_gen_state_dir() of
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

seq_gen_read_resume_value(Id) ->
    case seq_gen_state_dir() of
        "" ->
            0;
        Path ->
            try
                InFile = Path ++ "/" ++ integer_to_list(Id),
                {ok, Bin} = file:read_file(InFile),
                Value = binary_to_term(Bin),
                ?DEBUG("Id ~p resuming from value ~p\n", [Id, Value]),
                Value
            catch
                error:{badmatch, _} ->
                    0;
                X:Y ->
                    ?DEBUG("Error reading resume value for Id ~p ~p: ~p ~p\n",
                           [Id, Path ++ "/" ++ integer_to_list(Id), X, Y]),
                    0
            end
    end.

seq_gen_state_dir() ->
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
        Else ->
            case get(you_have_been_warned) of
                undefined ->
                    ?WARN("Config value ~p -> ~p is not an absolute "
                          "path, ignoring!\n", [Key, Else]),
                    put(you_have_been_warned, true);
                _ ->
                    ok
            end,
            ""
    end.

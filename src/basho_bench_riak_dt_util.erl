% -------------------------------------------------------------------
%%
%% basho_bench_riak_dt_util: Utilities functions for riak_dt related
%%                           bench-runs.
%%
%% Copyright (c) 2016 Basho Techonologies
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

-module(basho_bench_riak_dt_util).

-export([gen_set_batch/5,
         random_element/1,
         set_start_bin_size/1]).

-include("basho_bench.hrl").

%%%===================================================================
%%% Public Util Funs
%%%===================================================================

random_element([]) ->
    undefined;
random_element([E]) ->
   E;
random_element(Vals) ->
    Nth = crypto:rand_uniform(1, length(Vals)),
    lists:nth(Nth, Vals).

%% @doc starter binary size for set-preload.
set_start_bin_size(VGTup) ->
    StartBinSize =
        case VGTup of
            {var_bin_set, Lambda, _, poisson} ->
                basho_bench_valgen:poisson(Lambda);
            {var_bin_set, Lambda, LambdaThresh, _, poisson} ->
                basho_bench_valgen:poisson(Lambda, LambdaThresh);
            {var_bin_set, Min, Mean, _, exponential} ->
                basho_bench_valgen:exponential(Min, Mean);
            {var_bin_set, Min, Max, _} ->
                crypto:rand_uniform(Min, Max+1);
            {uniform_bin, Min, Max} ->
                crypto:rand_uniform(Min, Max+1);
            {exponential_bin, Min, Mean} ->
                basho_bench_valgen:exponential(Min, Mean);
            {fixed_bin_set, Size, _} ->
                Size;
            {_, Min, Max} when is_integer(Min), is_integer(Max) ->
                crypto:rand_uniform(Min, Max+1);
            {_, Size} when is_integer(Size) ->
                Size;
            _ -> 4
        end,
    ?DEBUG("StartBinSize: ~p\n", [StartBinSize]),
    StartBinSize.

%% @doc generate a tuple w/ a set-key and a batch of members from the valgen
%%      NOTE: to be used w/ non-sequential `key_generator` keygen, otherwise
%%      the reset for the exausted valgen will reset the key_generator
gen_set_batch(KeyGen, ValueGen, LastKey, BatchSize, SetValGenName) ->
    case {LastKey, gen_members(BatchSize, ValueGen)} of
        {_, []} ->
            %% Exhausted value gen, new key
            Key = KeyGen(),
            ?DEBUG("New set ~p~n", [Key]),
            case SetValGenName of
                undefined ->
                    basho_bench_keygen:reset_sequential_int_state();
                _ ->
                    basho_bench_keygen:reset_sequential_int_state(SetValGenName)
            end,
            {Key, gen_members(BatchSize, ValueGen)};
        {undefined, List} ->
            %% We have no active set, so generate a
            %% key. First run maybe
            Key = KeyGen(),
            ?DEBUG("New set ~p~n", [Key]),
            {Key, List};
        Else ->
            Else
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% @private generate as many elements as we can from the valgen, if it
%% exhausts, return the results we did get.
gen_members(BatchSize, ValueGen) ->
    accumulate_members(BatchSize, ValueGen, []).

%% @private generate as many elements as we can from the valgen, if it
%% exhausts, return the results we did get.
accumulate_members(0, _ValueGen, Acc) ->
    lists:reverse(Acc);
accumulate_members(BS, Gen, Acc) ->
    try
        accumulate_members(BS-1, Gen, [Gen() | Acc])
    catch throw:{stop, empty_keygen} ->
            ?DEBUG("ValGen exhausted~n", []),
            lists:reverse(Acc)
    end.


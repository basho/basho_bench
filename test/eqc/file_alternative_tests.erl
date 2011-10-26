%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2011 Gemini Mobile Technologies, Inc.  All rights reserved.
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
-module(file_alternative_tests).

-include_lib("eqc/include/eqc.hrl").

-export([run/0, run/1]).
%%-compile(export_all).

-define(DIR, "/tmp/erl_file_alternative").

run() ->
    run(100).

run(Num) ->
    os:cmd("rm -rf " ++ ?DIR),
    ok = file:make_dir(?DIR),

    Fails = 
        lists:dropwhile(
          fun(noop) -> true;
             (P) -> eqc:quickcheck(numtests(Num, P))
          end,
          [
           prop_delete(),
           prop_rename(),
           prop_mkdir(),
           prop_del_dir(),
           prop_list_dir(),
           prop_rename_dir(),
           noop
          ]),
    Fails.


%% -- props
prop_delete() ->
    ?FORALL(N, ?LAZY(gen_name()), 
            ?WHENFAIL(
               io:format(":::delete: ~p~n", [N]),
               delete(N))).

prop_rename() ->
    ?FORALL({N1, N2}, ?LAZY(gen_name(2)),
            ?WHENFAIL(
               io:format(":::rename: ~p/~p~n", [N1, N2]),
               rename(N1, N2))).

prop_mkdir() ->
    ?FORALL(N, ?LAZY(gen_name()),
            ?WHENFAIL(
               io:format(":::mkdir: ~p~n",[N]),
               mkdir(N))).

prop_del_dir() ->
    ?FORALL(N, ?LAZY(gen_name()),
            ?WHENFAIL(
               io:format(":::del_dir: ~p~n",[N]),
               del_dir(N))).

prop_list_dir() ->
    ?FORALL(N, ?LAZY(gen_name()),
            ?WHENFAIL(
               io:format(":::list_dir: ~p~n",[N]),
               list_dir(N))).

prop_rename_dir() ->
    ?FORALL({N1, N2}, ?LAZY(gen_name(2)),
            ?WHENFAIL(
               io:format(":::rename_dir: ~p/~p~n",[N1, N2]),
               rename_dir(N1,N2))).

%% -- tests
delete(N) ->
    F = filename:join([?DIR, N]),
    ok = file:write_file(F, <<>>),
    {ok,[N]} = file:list_dir(?DIR),
    ok = basho_bench_erlang_file_alternative:delete(F),
    {ok,[]} = file:list_dir(?DIR),
    true.

rename(N1, N2) ->
    F1 = filename:join([?DIR, N1]),
    F2 = filename:join([?DIR, N2]),
    ok = file:write_file(F1, <<>>),
    {ok, [N1]} = file:list_dir(?DIR),
    ok = basho_bench_erlang_file_alternative:rename(F1, F2),
    {ok, [N2]} = file:list_dir(?DIR),
    ok = file:delete(F2),
    true.

mkdir(N) ->
    D = filename:join([?DIR, N]),
    ok = basho_bench_erlang_file_alternative:make_dir(D),
    {ok,[N]} = file:list_dir(?DIR),
    ok = file:del_dir(D),
    {ok,[]} = file:list_dir(?DIR),
    true.

del_dir(N) ->
    D = filename:join([?DIR, N]),
    ok = file:make_dir(D),
    {ok,[N]} = file:list_dir(?DIR),
    ok = basho_bench_erlang_file_alternative:del_dir(D),
    {ok,[]} = file:list_dir(?DIR),
    true.

list_dir(N) ->
    D = filename:join([?DIR, N]),
    ok = file:make_dir(D),
    {ok,[N]} = basho_bench_erlang_file_alternative:list_dir(?DIR),
    ok = file:del_dir(D),
    {ok,[]} = basho_bench_erlang_file_alternative:list_dir(?DIR),
    true.

rename_dir(N1, N2) ->
    D1 = filename:join([?DIR, N1]),
    D2 = filename:join([?DIR, N2]),
    ok = file:make_dir(D1),
    {ok, [N1]} = file:list_dir(?DIR),
    ok = basho_bench_erlang_file_alternative:rename(D1, D2),
    {ok, [N2]} = file:list_dir(?DIR),
    ok = file:del_dir(D2),
    true.


%% -- gens
my_char() ->
    choose(0,255).

gen_name() ->
    ?SUCHTHAT(Name,gen_chars(),valid_name(Name)).

gen_chars() ->
    gen_chars(128).
gen_chars(Max) ->
    ?LET(Size, choose(1,Max),
         [?SUCHTHAT(C,my_char(),valid_char(C)) ||
             _ <- lists:seq(1,(Size+1))]).

gen_name(2) -> %% 2 different names
    ?LET(Name1, gen_name(),
         ?LET(Name2, ?SUCHTHAT(X,gen_name(),X /= Name1),
              {Name1, Name2})).

valid_char(0) ->
    false;
valid_char($/) ->
    false;
valid_char($|) ->
    false;
valid_char(_) ->
    true.

valid_name(".") ->
    false;
valid_name("..") ->
    false;
valid_name(_) ->
    true.

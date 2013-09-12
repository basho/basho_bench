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
-module(basho_bench_driver_kv_backend).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {
          backend :: atom(),
          bucket :: binary(),
          uses_r_object :: boolean(),
          be_state :: term()
         }).

%% ====================================================================
%% API
%% ====================================================================

%% @doc new
%%
%% Config items:
%%   - be_backend_mod :: atom()
%%   - be_disable_uses_r_object :: boolean()
%%   - be_config :: proplist(), passed to start/2

new(_Id) ->
    Backend = basho_bench_config:get(be_backend_mod, riak_kv_yessir_backend),
    io:format("DBG ~p\n", [Backend]),
    Bucket = <<"Oh, shouldn't matter much">>,
    {ok, Caps} = Backend:capabilities(x),
    UsesRObj = proplists:get_value(uses_r_object, Caps, false),
    RObjDisabled = basho_bench_config:get(be_disable_uses_r_object, false),
    Use = UsesRObj andalso not RObjDisabled,
    {ok, BE} = Backend:start(0, basho_bench_config:get(be_config, [])),

    {ok, #state{backend=Backend, bucket=Bucket, uses_r_object=Use,
                be_state=BE}}.

run(get, KeyGen, _ValueGen,
    #state{backend=Backend, bucket=Bucket, uses_r_object=RObjP,
           be_state=BE} = State) ->
    Key = KeyGen(),
    if RObjP ->
            {_ok_err, _, NewBE} = Backend:get_object(Bucket, Key, false, BE),
            {ok, State#state{be_state = NewBE}};
       true ->
            {_ok_err, _, NewBE} = Backend:get(Bucket, Key, BE),
            {ok, State#state{be_state = NewBE}}
       end;
run(put, KeyGen, ValueGen,
    #state{backend=Backend, bucket=Bucket, be_state=BE} = State) ->
    Key = KeyGen(),
    Val = ValueGen(),
    {ok, NewBE} = Backend:put(Bucket, Key, [], Val, BE),
    {ok, State#state{be_state = NewBE}};
run(do_something_else, KeyGen, ValueGen, State) ->
    _Key = KeyGen(),
    ValueGen(),
    {ok, State};
run(an_error, KeyGen, _ValueGen, State) ->
    _Key = KeyGen(),
    {error, went_wrong, State};
run(another_error, KeyGen, _ValueGen, State) ->
    _Key = KeyGen(),
    {error, {bad, things, happened}, State}.


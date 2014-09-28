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
-module(basho_bench_config).
-behaviour(gen_server).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-export([load/1,
         normalize_ips/2,
         set/2,
         get/1, get/2]).

-export([start_link/0]).

% Gen server callbacks
-export([code_change/3, init/1, terminate/2, handle_call/3]).

-include("basho_bench.hrl").

-record(basho_bench_config_state, {config_keys}).

-type state() :: #basho_bench_config_state{}.
%% ===================================================================
%% Public API
%% ===================================================================

ensure_started() -> 
    start_link().

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).


load(Files) ->
    ensure_started(),
    gen_server:call({global, ?MODULE}, {load_files, Files}). 
    
set(Key, Value) ->
    gen_server:call({global, ?MODULE}, {set, Key, Value}).

get(Key) ->
    case gen_server:call({global, ?MODULE}, {get, Key}) of
        {ok, Value} ->
            Value;
        error ->
            erlang:error("Missing configuration key", [Key])
    end.

get(Key, Default) ->
    case gen_server:call({global, ?MODULE}, {get, Key}) of
        {ok, Value} ->
            Value;
        error ->
            Default
    end.

%% @doc Normalize the list of IPs and Ports.
%%
%% E.g.
%%
%% ["127.0.0.1", {"127.0.0.1", 8091}, {"127.0.0.1", [8092,8093]}]
%%
%% => [{"127.0.0.1", DefaultPort},
%%     {"127.0.0.1", 8091},
%%     {"127.0.0.1", 8092},
%%     {"127.0.0.1", 8093}]
normalize_ips(IPs, DefultPort) ->
    F = fun(Entry, Acc) ->
                normalize_ip_entry(Entry, Acc, DefultPort)
        end,
    lists:foldl(F, [], IPs).





%% ===================================================================
%% Internal functions
%% ===================================================================


normalize_ip_entry({IP, Ports}, Normalized, _) when is_list(Ports) ->
    [{IP, Port} || Port <- Ports] ++ Normalized;
normalize_ip_entry({IP, Port}, Normalized, _) ->
    [{IP, Port}|Normalized];
normalize_ip_entry(IP, Normalized, DefaultPort) ->
    [{IP, DefaultPort}|Normalized].


%% ===
%% Gen_server Functions
%% ===

-spec init(term()) -> {ok, state()}.  
init(_Args) ->
    State = #basho_bench_config_state{config_keys = base_config()},
    {ok, State}.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.                                

-spec terminate(term(), state()) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

handle_call({load_files, FileNames}, _From, State) ->
  TermsList = get_keys_from_files(FileNames),
  ConfigKeys2 = load_termlist(TermsList, State#basho_bench_config_state.config_keys),
  State2 = State#basho_bench_config_state{config_keys = ConfigKeys2},
  {reply, ok, State2};
handle_call({set, app_run_mode, Value}, _From, State) ->
  application:set_env(basho_bench_app, app_run_mode, Value),
  {reply, ok, State};
handle_call({get, app_run_mode}, _From, State) ->
  case application:get_env(basho_bench_app, app_run_mode) of
    {ok, Value} ->
      {reply, {ok, Value}, State};
    undefined ->
      {reply, error, State}
  end;
handle_call({set, Key, Value}, _From, State) ->
  NewKVs = orddict:store(Key, Value, State#basho_bench_config_state.config_keys),
  State2 = State#basho_bench_config_state{config_keys = NewKVs},
  {reply, ok, State2};
handle_call({get, Key}, _From, State) ->
  Value = orddict:find(Key, State#basho_bench_config_state.config_keys),
  {reply, Value, State}.
get_keys_from_files(Files) ->
    [ case file:consult(File) of
          {ok, Terms} ->
              Terms;
          {error, Reason} ->
              ?FAIL_MSG("Failed to parse config file ~s: ~p\n", [File, Reason]),
              throw(invalid_config),
              notokay
      end || File <- Files ].


load_termlist(TermList, ExistingConfig) ->
    NewKVs = lists:flatten(TermList),
    FoldFun = fun({Key, Value}, Accum) ->
        orddict:store(Key, Value, Accum)
    end,
    lists:foldl(FoldFun, ExistingConfig, NewKVs).

base_config() ->
  orddict:from_list([
        %% Run mode: How should basho_bench started as a separate node, or part of an 
        %% other node. The default is standalone, other option is included.
   
         %%
         %% Mode of load generation:
         %% max - Generate as many requests as possible per worker
         %% {rate, Rate} - Exp. distributed Mean reqs/sec
         %%
         {mode, {rate, 5}},

         %%
         %% Default log level
         %%
         {log_level, debug},

         %%
         %% Base test output directory
         %%
         {test_dir, "tests"},

         %%
         %% Test duration (minutes)
         %%
         {duration, 5},

         %%
         %% Number of concurrent workers
         %%
         {concurrent, 3},

         %%
         %% Driver module for the current test
         %%
         {driver, basho_bench_driver_http_raw},

         %%
         %% Operations (and associated mix). Note that
         %% the driver may not implement every operation.
         %%
         {operations, [{get, 4},
                       {put, 4},
                       {delete, 1}]},

         %%
         %% Interval on which to report latencies and status (seconds)
         %%
         {report_interval, 10},

         %%
         %% Key generators
         %%
         %% {uniform_int, N} - Choose a uniformly distributed integer between 0 and N
         %%
         {key_generator, {uniform_int, 100000}},

         %%
         %% Value generators
         %%
         %% {fixed_bin, N} - Fixed size binary blob of N bytes
         %%
         {value_generator, {fixed_bin, 100}}
        ]).
-ifdef(TEST).
load_files_test() ->
  %% Extracted from bitcask, and null test.
  KVs = [[{mode,max},
       {duration,1},
       {report_interval,1},
       {concurrent,8},
       {driver,basho_bench_driver_null},
       {key_generator,{partitioned_sequential_int,5000000}},
       {disable_sequential_int_progress_report,true},
       {value_generator,{fixed_bin,10248}},
       {operations,[{do_something,7},{an_error,1},{another_error,2}]}],
      [{mode,max},
       {duration,10},
       {concurrent,1},
       {driver,basho_bench_driver_bitcask},
       {key_generator,{int_to_bin_bigendian,{uniform_int,5000000}}},
       {value_generator,{fixed_bin,10000}},
       {operations,[{get,1},{put,1}]},
       {code_paths,["../../public/bitcask"]},
       {bitcask_dir,"/tmp/bitcask.bench"},
       {bitcask_flags,[o_sync]}]],
  KVOrdDict = [{bitcask_dir,"/tmp/bitcask.bench"},
      {bitcask_flags,[o_sync]},
      {code_paths,["../../public/bitcask"]},
      {concurrent,1},
      {disable_sequential_int_progress_report,true},
      {driver,basho_bench_driver_bitcask},
      {duration,10},
      {key_generator,{int_to_bin_bigendian,{uniform_int,5000000}}},
      {mode,max},
      {operations,[{get,1},{put,1}]},
      {report_interval,1},
      {value_generator,{fixed_bin,10000}}],
  ?assertEqual(KVOrdDict, load_termlist(KVs, [])).
-endif.

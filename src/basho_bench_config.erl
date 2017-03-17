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

-export([
    get/1,
    get/2,
    load/1,
    normalize_ips/2,
    set/2,
    set_local_config/1
]).

-export([start_link/0]).

% Gen server callbacks
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).


-include("basho_bench.hrl").


-record(config_state, {
    workers
}).


-type state() :: #config_state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% Todo: ensure_started before calling on any gen_server APIs.
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
    case get_local_config(Key) of
        undefined ->
            case get_global_config(Key) of
                {ok, Value} ->
                    Value;
                undefined ->
                    erlang:error("Missing configuration key", [Key])
            end;
        {ok, Value} ->
            Value
    end.


get(Key, Default) ->
    case get_local_config(Key) of
        undefined ->
            case get_global_config(Key) of
                {ok, Value} ->
                    Value;
                undefined ->
                    Default
            end;
        {ok, Value} ->
            Value
    end.


set_local_config(LocalConfig) when is_list(LocalConfig) ->
    Map = map_from_list(LocalConfig),
    set_local_config(Map);
set_local_config(LocalConfig) when is_map(LocalConfig) ->
    erlang:put(local_config, LocalConfig).


%% TODO: Change to maps:from_list(List) in Erlang 18
map_from_list(List) ->
    map_from_list(List, #{}).


map_from_list([], Map) ->
    Map;
map_from_list([{K, V} | Rest], Map) ->
    map_from_list(Rest, maps:put(K, V, Map)).


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
    lists:foldl(
        fun(Entry, Acc) ->
                normalize_ip_entry(Entry, Acc, DefultPort)
        end, [], IPs).


%% ===================================================================
%% Internal functions
%% ===================================================================


get_local_config(Key) ->
    get_local_config(Key, undefined).


get_local_config(Key, Default) ->
    case erlang:get(local_config) of
        undefined ->
            Default;
        Conf ->
            case maps:get(Key, Conf, undefined) of
                undefined -> Default;
                Value -> {ok, Value}
            end
    end.


get_global_config(Key) ->
    gen_server:call({global, ?MODULE}, {get, Key}).


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
    State = #config_state{ workers=[]},
    {ok, State}.


-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-spec terminate(term(), state()) -> 'ok'.
terminate(_Reason, _State) ->
    ok.


handle_call({load_files, FileNames}, _From, State) ->
    set_keys_from_files(FileNames),
    {reply, ok, State};


handle_call({set, Key, Value}, _From, State) ->
    application:set_env(basho_bench, Key, Value), 
    {reply, ok, State};

handle_call({get, Key}, _From, State) ->
    Value = application:get_env(basho_bench, Key),
    {reply, Value, State}.


handle_cast(_Cast, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


set_keys_from_files(Files) ->
    KVs = lists:map(
        fun(File) ->
            case file:consult(File) of
                {ok, Terms} ->
                    Terms;
                {error, Reason} ->
                    ?FAIL_MSG("Failed to parse config file ~s: ~p\n", [File, Reason]),
                    throw(invalid_config),
                    notokay
           end
	end, Files),
    FlatKVs = lists:flatten(KVs),
    [application:set_env(basho_bench, Key, Value) || {Key, Value} <- FlatKVs].

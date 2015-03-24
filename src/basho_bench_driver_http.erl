%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2013 Basho Techonologies
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
-module(basho_bench_driver_http).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(url, {abspath, host, port, username, password, path, protocol, host_type}).

-record(state, {
          generators = [],
          values = [],
          headers = [],
          targets = [],
          target_indexes = []}).


%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    ?DEBUG("ID: ~p\n", [Id]),

    %% Make sure ibrowse is available
    case code:which(ibrowse) of
        non_existing ->
            ?FAIL_MSG("~s requires ibrowse to be installed.\n", [?MODULE]);
        _ ->
            ok
    end,

    application:start(ibrowse),

    Disconnect = basho_bench_config:get(http_disconnect_frequency, infinity),

    case Disconnect of
        infinity -> ok;
        Seconds when is_integer(Seconds) -> ok;
        {ops, Ops} when is_integer(Ops) -> ok;
        _ -> ?FAIL_MSG("Invalid configuration for http_disconnect_frequency: ~p~n", [Disconnect])
    end,

    %% Uses pdict to avoid threading state record through lots of functions
    erlang:put(disconnect_freq, Disconnect),

    %% TODO: Validate these
    Generators = build_generators(basho_bench_config:get(generators), [], Id),
    Values = basho_bench_config:get(values),
    Headers = basho_bench_config:get(headers),
    TargetsProplist = basho_bench_config:get(targets),
    TargetIndexes = build_target_list(TargetsProplist, []),

    {ok, #state {
            generators = Generators,
            values = Values,
            headers = Headers,
            targets = TargetsProplist,
            target_indexes = TargetIndexes
           }}.

run({get, Target}, KeyGen, ValueGen, State) ->
    run({get, Target, undefined}, KeyGen, ValueGen, State);
run({get, Target, HeaderName}, KeyGen, ValueGen, State) ->
    {Url, S2} = next_url(Target, KeyGen, ValueGen, State),
    Headers = proplists:get_value(HeaderName, S2#state.headers, []),

    case do_get(Url, Headers) of
        {not_found, _Url} ->
            {ok, S2};
        {ok, _Url, _Header} ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run({put, Target, ValueName}, KeyGen, ValueGen, State) ->
    run({put, Target, ValueName, undefined}, KeyGen, ValueGen, State);
run({put, Target, ValueName, HeaderName}, KeyGen, ValueGen, State) ->
    {Url, S2} = next_url(Target, KeyGen, ValueGen, State),
    Headers = proplists:get_value(HeaderName, S2#state.headers, []),
    Data = build_value(ValueName, KeyGen, ValueGen, S2),

    case do_put(Url, Headers, Data) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run({post, Target, ValueName}, KeyGen, ValueGen, State) ->
    run({post, Target, ValueName, undefined}, KeyGen, ValueGen, State);
run({post, Target, ValueName, HeaderName}, KeyGen, ValueGen, State) ->
    {Url, S2} = next_url(Target, KeyGen, ValueGen, State),
    Headers = proplists:get_value(HeaderName, S2#state.headers, []),
    Data = build_value(ValueName, KeyGen, ValueGen, S2),

    case do_post(Url, Headers, Data) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run({delete, Target}, KeyGen, ValueGen, State) ->
    run({delete, Target, undefined}, KeyGen, ValueGen, State);
run({delete, Target, HeaderName}, KeyGen, ValueGen, State) ->
    {Url, S2} = next_url(Target, KeyGen, ValueGen, State),
    Headers = proplists:get_value(HeaderName, S2#state.headers, []),

    case do_delete(Url, Headers) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

build_generators([{Name, {key_generator, KeyGenSpec}}|Rest], Generators, Id) ->
    KeyGen = basho_bench_keygen:new(KeyGenSpec, Id),
    build_generators(Rest, [{Name, KeyGen}|Generators], Id);
build_generators([{Name, {value_generator, ValGenSpec}}|Rest], Generators, Id) ->
    ValGen = basho_bench_valgen:new(ValGenSpec, Id),
    build_generators(Rest, [{Name, ValGen}|Generators], Id);
build_generators([], Generators, _) ->
    Generators.

evaluate_generator(Name, Generators, KeyGen, ValueGen) ->
    case Name of
        key_generator -> KeyGen();
        value_generator -> ValueGen();
        N when is_atom(N) ->
            Fun = proplists:get_value(N, Generators),
            Fun();
        Value -> Value
    end.

build_formatted_value(String, GeneratorNames, Generators, KeyGen, ValueGen) ->
    Values = lists:map(fun (Name) -> evaluate_generator(Name, Generators, KeyGen, ValueGen) end, GeneratorNames),
    io_lib:format(String, Values).

%% Round robin sub-target selection
next_url({TargetName, Index, Targets}, KeyGen, ValueGen, State) 
        when is_list(Targets), Index > length(Targets) ->
    OtherIndexes = proplists:delete(TargetName, State#state.target_indexes),
    S2 = State#state{target_indexes = [{TargetName, 1} | OtherIndexes]},
    next_url({TargetName, 1, Targets}, KeyGen, ValueGen, S2);
next_url({TargetName, Index, Targets}, KeyGen, ValueGen, State) 
        when is_list(Targets) ->
    OtherIndexes = proplists:delete(TargetName, State#state.target_indexes),
    Url = build_url(lists:nth(Index, Targets), State#state.generators, KeyGen, ValueGen),
    S2 = State#state{target_indexes = [{TargetName, Index + 1} | OtherIndexes]},
    {Url, S2};
next_url({_, _, Target}, KeyGen, ValueGen, State) when is_tuple(Target) ->
    Url = build_url(Target, State#state.generators, KeyGen, ValueGen),
    {Url, State};
next_url(TargetName, KeyGen, ValueGen, State) when is_atom(TargetName) ->
    Index = proplists:get_value(TargetName, State#state.target_indexes), 
    Target = proplists:get_value(TargetName, State#state.targets),
    next_url({TargetName, Index, Target}, KeyGen, ValueGen, State).

build_url({Host, Port, {FormattedPath, GeneratorNames}}, Generators, KeyGen, ValueGen) ->
    Path = build_formatted_value(FormattedPath, GeneratorNames, Generators, KeyGen, ValueGen),
    #url{host=Host, port=Port, path=Path};
build_url({Host, Port, Path}, _, _, _) ->
    #url{host=Host, port=Port, path=Path}.

build_target_list([], TargetIndexes) ->
    TargetIndexes;
build_target_list([{Name, _}|Rest], TargetIndexes) ->
    build_target_list(Rest, [{Name, 1} | TargetIndexes]).

build_value(ValueName, KeyGen, ValueGen, State) ->
    case proplists:get_value(ValueName, State#state.values) of
        {FormattedValue, GeneratorNames} ->
            build_formatted_value(FormattedValue, GeneratorNames, State#state.generators, KeyGen, ValueGen);
        V -> evaluate_generator(V, State#state.generators, KeyGen, ValueGen)
    end.

do_get(Url, Headers) ->
    case send_request(Url, Headers, get, [], [{response_format, binary}]) of
        {ok, "404", _Header, _Body} ->
            {not_found, Url};
        {ok, "300", Header, _Body} ->
            {ok, Url, Header};
        {ok, "200", Header, _Body} ->
            {ok, Url, Header};
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_put(Url, Headers, ValueGen) ->
    Val = if is_function(ValueGen) ->
                  ValueGen();
             true ->
                  ValueGen
          end,
    case send_request(Url, Headers,
                      put, Val, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "201", _Header, _Body} ->
            ok;
        {ok, "202", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_post(Url, Headers, ValueGen) ->
    Val = if is_function(ValueGen) ->
                  ValueGen();
             true ->
                  ValueGen
          end,
    case send_request(Url, Headers,
                      post, Val, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "201", _Header, _Body} ->
            ok;
        {ok, "202", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete(Url, Headers) ->
    case send_request(Url, Headers, delete, [], []) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "201", _Header, _Body} ->
            ok;
        {ok, "202", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, "404", _Header, _Body} ->
            ok;
        {ok, "410", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

connect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            {ok, Pid} = ibrowse_http_client:start({Url#url.host, Url#url.port}),
            erlang:put({ibrowse_pid, Url#url.host}, Pid),
            Pid;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    erlang:erase({ibrowse_pid, Url#url.host}),
                    connect(Url)
            end
    end.


disconnect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            ok;
        OldPid ->
            catch(ibrowse_http_client:stop(OldPid))
    end,
    erlang:erase({ibrowse_pid, Url#url.host}),
    ok.

maybe_disconnect(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, Count} -> should_disconnect_ops(Count,Url) andalso disconnect(Url);
        Seconds -> should_disconnect_secs(Seconds,Url) andalso disconnect(Url)
    end.

should_disconnect_ops(Count, Url) ->
    Key = {ops_since_disconnect, Url#url.host},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, 1),
            false;
        Count ->
            erlang:put(Key, 0),
            true;
        Incr ->
            erlang:put(Key, Incr + 1),
            false
    end.

should_disconnect_secs(Seconds, Url) ->
    Key = {last_disconnect, Url#url.host},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, erlang:now()),
            false;
        Time when is_tuple(Time) andalso size(Time) == 3 ->
            Diff = timer:now_diff(erlang:now(), Time),
            if
                Diff >= Seconds * 1000000 ->
                    erlang:put(Key, erlang:now()),
                    true;
                true -> false
            end
    end.

clear_disconnect_freq(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, _Count} -> erlang:put({ops_since_disconnect, Url#url.host}, 0);
        _Seconds -> erlang:put({last_disconnect, Url#url.host}, erlang:now())
    end.

send_request(Url, Headers, Method, Body, Options) ->
    send_request(Url, Headers, Method, Body, Options, 3).

send_request(_Url, _Headers, _Method, _Body, _Options, 0) ->
    {error, max_retries};
send_request(Url, Headers, Method, Body, Options, Count) ->
    Pid = connect(Url),
    case catch(ibrowse_http_client:send_req(Pid, Url, Headers ++ basho_bench_config:get(http_append_headers,[]), Method, Body, Options, basho_bench_config:get(http_request_timeout, 5000))) of
        {ok, Status, RespHeaders, RespBody} ->
            maybe_disconnect(Url),
            {ok, Status, RespHeaders, RespBody};

        Error ->
            clear_disconnect_freq(Url),
            disconnect(Url),
            case should_retry(Error) of
                true ->
                    send_request(Url, Headers, Method, Body, Options, Count-1);

                false ->
                    normalize_error(Method, Error)
            end
    end.


should_retry({error, send_failed})       -> true;
should_retry({error, connection_closed}) -> true;
should_retry({'EXIT', {normal, _}})      -> true;
should_retry({'EXIT', {noproc, _}})      -> true;
should_retry(_)                          -> false.

normalize_error(Method, {'EXIT', {timeout, _}})  -> {error, {Method, timeout}};
normalize_error(Method, {'EXIT', Reason})        -> {error, {Method, 'EXIT', Reason}};
normalize_error(Method, {error, Reason})         -> {error, {Method, Reason}}.

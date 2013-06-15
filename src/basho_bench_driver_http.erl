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

-record(state, {path_params}).        % Params to append on the path


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

    Params = basho_bench_config:get(http_params, ""),
    Disconnect = basho_bench_config:get(http_disconnect_frequency, infinity),
    
    case Disconnect of
        infinity -> ok;
        Seconds when is_integer(Seconds) -> ok;
        {ops, Ops} when is_integer(Ops) -> ok;
        _ -> ?FAIL_MSG("Invalid configuration for http_disconnect_frequency: ~p~n", [Disconnect])
    end,

    %% Uses pdict to avoid threading state record through lots of functions
    erlang:put(disconnect_freq, Disconnect),

    {ok, #state {path_params = Params}}.

run({get_re, {Host, Port, Path}, Headers}, KeyGen, _ValueGen, _State) ->
    Path1 = re:replace(Path, "%%K", KeyGen(), [global, {return, list}]),
    run({get, {Host, Port, Path1}, Headers}, KeyGen, _ValueGen, _State);

run({get, {Host, Port, Path}, Headers}, _KeyGen, _ValueGen, _State) ->
    PUrl = #url{host=Host, port=Port, path=Path},

    case do_get(PUrl, Headers) of
        {not_found, _Url} ->
            {ok, 1};
        {ok, _Url, _Header} ->
            {ok, 1};
        {error, Reason} ->
            {error, Reason, 1}
    end;

run({put_re, {Host, Port, Path, Data}, Headers}, KeyGen, ValueGen, _State) ->
    Path1 = re:replace(Path, "%%K", KeyGen(), [global, {return, list}]),
    Value = re:replace(Data, "%%V", ValueGen(), [global, {return, list}]),
    run({put, {Host, Port, Path1, Value}, Headers}, KeyGen, ValueGen, _State);

run({put, {Host, Port, Path, Data}, Headers}, _KeyGen, _ValueGen, _State) ->
    PUrl = #url{host=Host, port=Port, path=Path},

    case do_put(PUrl, Headers, Data) of
        ok ->
            {ok, 1};
        {error, Reason} ->
            {error, Reason, 1}
    end;

run({post_re, {Host, Port, Path, Data}, Headers}, KeyGen, ValueGen, _State) ->
    Path1 = re:replace(Path, "%%K", KeyGen(), [global, {return, list}]),
    Value = re:replace(Data, "%%V", ValueGen(), [global, {return, list}]),
    run({post, {Host, Port, Path1, Value}, Headers}, KeyGen, ValueGen, _State);

run({post, {Host, Port, Path, Data}, Headers}, _KeyGen, _ValueGen, _State) ->
    PUrl = #url{host=Host, port=Port, path=Path},

    case do_post(PUrl, Headers, Data) of
        ok ->
            {ok, 1};
        {error, Reason} ->
            {error, Reason, 1}
    end;

run({delete_re, {Host, Port, Path}, Headers}, KeyGen, _ValueGen, _State) ->
    Path1 = re:replace(Path, "%%K", KeyGen(), [global, {return, list}]),
    run({delete, {Host, Port, Path1}, Headers}, KeyGen, _ValueGen, _State);

run({delete, {Host, Port, Path}, Headers}, _KeyGen, _ValueGen, _State) ->
    PUrl = #url{host=Host, port=Port, path=Path},

    case do_delete(PUrl, Headers) of
        ok ->
            {ok, 1};
        {error, Reason} ->
            {error, Reason, 1}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

do_get(Url, Headers) ->
    %%case send_request(Url, [], get, [], [{response_format, binary}]) of
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

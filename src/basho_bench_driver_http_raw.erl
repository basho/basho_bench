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
-module(basho_bench_driver_http_raw).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(url, {abspath, host, port, username, password, path, protocol, host_type}).

-record(state, { client_id,          % Tuple client ID for HTTP requests
                 base_urls,          % Tuple of #url -- one for each IP
                 base_urls_index,    % #url to use for next request
                 files,              % List of files to put
                 path_params,        % Params to append on the path
                 solr_path,          % SOLR path for searches
                 searchgen,          % Search generator
                 opt_ssl_options,
                 opt_raw_headers,
                 opt_request_timeout
               }).


-define(NOT_EXPECTED(Qry, Expected, Actual),
        lists:flatten(io_lib:format("Query ~p expected ~p but got ~p",
                                    [Qry, Expected, Actual]))).


%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure ibrowse is available
    case code:which(ibrowse) of
        non_existing ->
            ?FAIL_MSG("~s requires ibrowse to be installed.\n", [?MODULE]);
        _ ->
            ok
    end,

    case basho_bench_config:get(http_use_ssl, false) of
        false ->
            ok;
        _ ->
            case ssl:start() of
                ok ->
                    ok;
                {error, {already_started, ssl}} ->
                    ok;
                _ ->
                    ?FAIL_MSG("Unable to enable SSL support.\n", [])
            end
    end,

    %% Setup client ID by base-64 encoding the ID
    ClientId = {'X-Riak-ClientId', base64:encode(<<Id:32/unsigned>>)},

    application:start(ibrowse),

    %% The IPs, port and path we'll be testing
    Ips  = basho_bench_config:get(http_raw_ips, ["127.0.0.1"]),
    DefaultPort = basho_bench_config:get(http_raw_port, 8098),
    Path = basho_bench_config:get(http_raw_path, "/riak/test"),
    Params = basho_bench_config:get(http_raw_params, ""),
    Disconnect = basho_bench_config:get(http_raw_disconnect_frequency, infinity),
    SolrPath = basho_bench_config:get(http_solr_path, "/solr/test"),
    SearchGen = case basho_bench_config:get(http_search_generator, undefined) of
                    undefined ->
                        undefined;
                    V ->
                        searchgen(V, ClientId)
                end,
    FileDir = basho_bench_config:get(file_dir, undefined),
    Files = case FileDir of
                undefined -> undefined;
                _ -> filelib:wildcard(FileDir ++ "/*")
            end,

    Warn = fun(X) ->
                   if Id == 1 ->
                           Msg = "Warning: {http_raw_disconnect_frequency, ~w}: ibrowse client can bottleneck on inet_gethost_native service when using disconnecting mode; 'infinity' is recommended\n",
                           io:format(user, "\n\n" ++ Msg ++ "\n\n", [X]),
                           error_logger:warning_msg(Msg, [X]),
                           timer:sleep(2*1000);
                      true ->
                           ok
                   end
           end,
    case Disconnect of
        infinity -> ok;
        0 -> ok;
        Seconds when is_integer(Seconds) -> Warn(Seconds), ok;
        {ops, 0} -> ok;
        {ops, Ops}=OpsT when is_integer(Ops) -> Warn(OpsT), ok;
        _ -> ?FAIL_MSG("Invalid configuration for http_raw_disconnect_frequency: ~p~n", [Disconnect])
    end,

    %% Uses pdict to avoid threading state record through lots of functions
    erlang:put(disconnect_freq, Disconnect),

    %% Convert the list to a tuple so we can efficiently round-robin
    %% through them.
    Targets = basho_bench_config:normalize_ips(Ips, DefaultPort),
    BaseUrls = list_to_tuple([#url{host=IP, port=Port, path=Path}
                              || {IP, Port} <- Targets,
                                 sanity_check_hostname(IP)]),
    BaseUrlsIndex = rand:uniform(tuple_size(BaseUrls)),

    SSL_options = case basho_bench_config:get(http_use_ssl, false) of
                      false ->
                          [];
                      true ->
                          [{is_ssl, true}, {ssl_options, []}];
                      SSLOpts when is_list(SSLOpts) ->
                          [{is_ssl, true}, {ssl_options, SSLOpts}]
                  end,
    RawHeaders = basho_bench_config:get(http_raw_append_headers,[]),
    RequestTimeout = basho_bench_config:get(http_raw_request_timeout, 5000),

    {ok, #state { client_id = ClientId,
                  base_urls = BaseUrls,
                  base_urls_index = BaseUrlsIndex,
                  files = Files,
                  path_params = Params,
                  solr_path = SolrPath,
                  searchgen = SearchGen,
                  opt_ssl_options = SSL_options,
                  opt_raw_headers = RawHeaders,
                  opt_request_timeout = RequestTimeout
                }}.

run(stat, _, _, State) ->
    {Url, S2} = next_url(State),
    case do_stat(stat_url(Url), State) of
        {ok, _, _} ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run(get, KeyGen, _ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    case do_get(url(NextUrl, KeyGen, State#state.path_params), State) of
        {not_found, _Url} ->
            {ok, S2};
        {ok, _Url, _Headers} ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;
run(get_existing, KeyGen, _ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    case do_get(url(NextUrl, KeyGen, State#state.path_params), State) of
        {not_found, Url} ->
            {error, {not_found, Url}, S2};
        {ok, _Url, _Headers} ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;
run(update, KeyGen, ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    case do_get(url(NextUrl, KeyGen, State#state.path_params), State) of
        {error, Reason} ->
            {error, Reason, S2};

        {not_found, Url} ->
            case do_put(Url, [], ValueGen, State) of
                ok ->
                    {ok, S2};
                {error, Reason} ->
                    {error, Reason, S2}
            end;

        {ok, Url, Headers} ->
            Vclock = lists:keyfind("X-Riak-Vclock", 1, Headers),
            case do_put(Url, [State#state.client_id, Vclock], ValueGen, State) of
                ok ->
                    {ok, S2};
                {error, Reason} ->
                    {error, Reason, S2}
            end
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    case do_get(url(NextUrl, KeyGen, State#state.path_params), State) of
        {error, Reason} ->
            {error, Reason, S2};

        {not_found, Url} ->
            {error, {not_found, Url}, S2};

        {ok, Url, Headers} ->
            Vclock = lists:keyfind("X-Riak-Vclock", 1, Headers),
            case do_put(Url, [State#state.client_id, Vclock], ValueGen, State) of
                ok ->
                    {ok, S2};
                {error, Reason} ->
                    {error, Reason, S2}
            end
    end;
run(insert, KeyGen, ValueGen, State) ->
    %% Go ahead and evaluate the keygen so that we can use the
    %% sequential_int_gen to do a controlled # of inserts (if we desire). Note
    %% that the actual insert randomly generates a key (server-side), so the
    %% output of the keygen is ignored.
    KeyGen(),
    {NextUrl, S2} = next_url(State),
    case do_post(url(NextUrl, State#state.path_params), [], ValueGen, State) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;
run(put, KeyGen, ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    Url = url(NextUrl, KeyGen, State#state.path_params),
    case do_put(Url, [State#state.client_id], ValueGen, State) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    Url = url(NextUrl, KeyGen, State#state.path_params),
    case do_delete(Url, [State#state.client_id], State) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run(put_file, _, _, #state{files=[]}) ->
    throw({stop, empty_keygen});

run(put_file, _, _, State) ->
    {NextUrl, S2} = next_url(State),
    [File|RemainingFiles] = State#state.files,
    S3 = S2#state{files=RemainingFiles},
    Key = filename:basename(File),
    {ok, Val} = file:read_file(File),
    Url = url(NextUrl, Key, State#state.path_params),
    case do_put(Url, [State#state.client_id], Val, State) of
        ok -> {ok, S3};
        {error, Reason} -> {error, Reason, S3}
    end;

run({search, {Qry, Expected}}, _, _, State) ->
    {NextUrl, S2} = next_url(State),
    SolrPath = State#state.solr_path,
    Encoded = mochiweb_util:urlencode([{q, Qry}, {wt, "json"}, {fl, "id"}]),
    SearchUrl = search_url(NextUrl, SolrPath, Encoded),
    Res = do_get(SearchUrl, [{body_on_success, true}], State),
    case Res of
        {ok, _, _, Body} ->
            Struct = mochijson2:decode(Body),
            case check_numfound(Struct, Expected) of
                true ->
                    {ok, S2};
                {false, Actual} ->
                    {error, ?NOT_EXPECTED(Qry, Expected, Actual), S2}
            end;
        {error, Reason} ->
            {error, Reason, S2}
    end;

run(search, _KeyGen, _ValueGen, State) when State#state.searchgen == undefined ->
    {_NextUrl, S2} = next_url(State),
    {error, {badarg, "http_search_generator needed for search operation"}, S2};

run(search, _KeyGen, _ValueGen, State) ->
    %% Handle missing searchgen
    {NextUrl, S2} = next_url(State),
    SearchUrl = search_url(NextUrl, State#state.solr_path, State#state.searchgen),
    SearchRes = do_get(SearchUrl, State),
    case SearchRes of
        {ok, _Url, _Headers} ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end.


%% ====================================================================
%% Search Generator API
%% ====================================================================

searchgen({function, Module, Function, Args}, Id) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            erlang:apply(Module, Function, [Id] ++ Args);
        _Error ->
            ?FAIL_MSG("Could not find searchgen function: ~p:~p\n", [Module, Function])
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

check_numfound(Struct, Expected) ->
    NumFound = get_path(Struct, [<<"response">>, <<"numFound">>]),
    if Expected == NumFound ->
            true;
       true ->
            {false, NumFound}
    end.

get_path({struct, PL}, Path) ->
    get_path(PL, Path);
get_path(PL, [Name]) ->
    case proplists:get_value(Name, PL) of
        {struct, Obj} -> Obj;
        Val -> Val
    end;
get_path(PL, [Name|Path]) ->
    get_path(proplists:get_value(Name, PL), Path).

next_url(State) when is_record(State#state.base_urls, url) ->
    {State#state.base_urls, State};
next_url(State) when State#state.base_urls_index > tuple_size(State#state.base_urls) ->
    next_url(State#state { base_urls_index = 1 });
next_url(State) ->
    { element(State#state.base_urls_index, State#state.base_urls),
      State#state { base_urls_index = State#state.base_urls_index + 1 }}.

url(BaseUrl, Params) ->
    BaseUrl#url { path = lists:concat([BaseUrl#url.path, Params]) }.
url(BaseUrl, KeyGen, Params) when is_function(KeyGen) ->
    BaseUrl#url { path = lists:concat([BaseUrl#url.path, '/', KeyGen(), Params]) };
url(BaseUrl, Key, Params) ->
    BaseUrl#url { path = lists:concat([BaseUrl#url.path, '/', Key, Params]) }.

search_url(BaseUrl, SolrPath, SearchGen) ->
    Params = if is_function(SearchGen) ->
                     SearchGen();
                true ->
                     SearchGen
             end,
    BaseUrl#url { path = lists:concat([SolrPath, '/select?', Params]) }.

stat_url(BaseUrl) ->
    BaseUrl#url{path="/stats"}.

do_stat(Url, S) ->
    case send_request(Url, [], get, [], [{response_format, binary}], S) of
        {ok, "200", Headers, _Body} ->
            {ok, Url, Headers};
        {ok, Code, _, _} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_get(Url, S) ->
    do_get(Url, [], S).

do_get(Url, Opts, S) ->
    case send_request(Url, [], get, [], [{response_format, binary}], S) of
        {ok, "404", _Headers, _Body} ->
            {not_found, Url};
        {ok, "300", Headers, _Body} ->
            {ok, Url, Headers};
        {ok, "200", Headers, Body} ->
            case proplists:get_bool(body_on_success, Opts) of
                true -> {ok, Url, Headers, Body};
                false -> {ok, Url, Headers}
            end;
        {ok, Code, _Headers, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_put(Url, Headers, ValueGen, S) ->
    Val = if is_function(ValueGen) ->
                  ValueGen();
             true ->
                  ValueGen
          end,
    case send_request(Url, Headers ++ [{'Content-Type', 'application/octet-stream'}],
                      put, Val, [{response_format, binary}], S) of
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_post(Url, Headers, ValueGen, S) ->
    case send_request(Url, Headers ++ [{'Content-Type', 'application/octet-stream'}],
                      post, ValueGen(), [{response_format, binary}], S) of
        {ok, "201", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete(Url, Headers, S) ->
    case send_request(Url, Headers, delete, [], [], S) of
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, "404", _Header, _Body} ->
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

should_disconnect_ops(Count, _Url) when Count =< 0 ->
    false;
should_disconnect_ops(Count, _Url) when Count =:= 1 ->
    true;
should_disconnect_ops(Count, Url) ->
    Key = {ops_since_disconnect, Url#url.host},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, 1),
            false;
        CountUntilLastOne when CountUntilLastOne =:= Count - 1 ->
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
            erlang:put(Key, os:timestamp()),
            false;
        Time when is_tuple(Time) andalso size(Time) == 3 ->
            Diff = timer:now_diff(os:timestamp(), Time),
            if
                Diff >= Seconds * 1000000 ->
                    erlang:put(Key, os:timestamp()),
                    true;
                true -> false
            end
    end.

clear_disconnect_freq(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, _Count} -> erlang:put({ops_since_disconnect, Url#url.host}, 0);
        _Seconds -> erlang:put({last_disconnect, Url#url.host}, os:timestamp())
    end.

send_request(Url, Headers, Method, Body, Options, S) ->
    send_request(Url, Headers, Method, Body, Options, 3, S).

send_request(_Url, _Headers, _Method, _Body, _Options, 0, _S) ->
    {error, max_retries};
send_request(Url, Headers, Method, Body, Options, Count, S) ->
    Pid = connect(Url),
    SockOpts = [{reuseaddr, true}, {nodelay, true}, {delay_send, false},
                S#state.opt_ssl_options],
    Options2 = Options ++ SockOpts,
    case catch(ibrowse_http_client:send_req(Pid, Url, Headers ++ S#state.opt_raw_headers, Method, Body, Options2, S#state.opt_request_timeout)) of
        {ok, Status, RespHeaders, RespBody} ->
            maybe_disconnect(Url),
            {ok, Status, RespHeaders, RespBody};

        Error ->
            clear_disconnect_freq(Url),
            disconnect(Url),
            case should_retry(Error) of
                true ->
                    send_request(Url, Headers, Method, Body, Options, Count-1, S);

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

sanity_check_hostname(Str) when is_list(Str) ->
    true;
sanity_check_hostname(T) when is_tuple(T) ->
    error_logger:warning_msg("Error: ~w: ibrowse client cannot handle Erlang tuple-style IP addresses, please use hostname/IP as a string\n", [T]),
    timer:sleep(1000),
    exit(bad_config).

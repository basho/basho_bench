%% -------------------------------------------------------------------
%%
%% riak_bench: Benchmarking Suite for Riak
%%
%% Copyright (c) 2009 Basho Techonologies
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
-module(riak_bench_driver_http_raw).

-export([new/1,
         run/4]).

-include("riak_bench.hrl").

-record(url, {abspath, host, port, username, password, path, protocol}).

-record(state, { client_id,          % Tuple client ID for HTTP requests
                 base_urls,          % Tuple of #url -- one for each IP
                 base_urls_index }). % #url to use for next request

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

    %% Setup client ID by base-64 encoding the ID
    ClientId = {'X-Riak-ClientId', base64:encode(<<Id:32/unsigned>>)},
    ?CONSOLE("Client ID: ~p\n", [ClientId]),

    application:start(ibrowse),

    %% The IPs, port and path we'll be testing
    Ips  = riak_bench_config:get(http_raw_ips, ["127.0.0.1"]),
    Port = riak_bench_config:get(http_raw_port, 8098),
    Path = riak_bench_config:get(http_raw_path, "/raw/test"),

    %% If there are multiple URLs, convert the list to a tuple so we can efficiently
    %% round-robin through them.
    case length(Ips) of
        1 ->
            [Ip] = Ips,
            BaseUrls = #url { host = Ip, port = Port, path = Path },
            BaseUrlsIndex = 1;
        _ ->
            BaseUrls = list_to_tuple([ #url { host = Ip, port = Port, path = Path}
                                       || Ip <- Ips]),
            BaseUrlsIndex = random:uniform(tuple_size(BaseUrls))
    end,

    {ok, #state { client_id = ClientId,
                  base_urls = BaseUrls,
                  base_urls_index = BaseUrlsIndex }}.


run(get, KeyGen, _ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    case do_get(NextUrl, KeyGen) of
        {not_found, _Url} ->
            {ok, S2};
        {ok, _Url, _Headers} ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;
run(update, KeyGen, ValueGen, State) ->
    {NextUrl, S2} = next_url(State),
    case do_get(NextUrl, KeyGen) of
        {error, Reason} ->
            {error, Reason, S2};

        {not_found, Url} ->
            case do_put(Url, [], ValueGen) of
                ok ->
                    {ok, S2};
                {error, Reason} ->
                    {error, Reason, S2}
            end;

        {ok, Url, Headers} ->
            Vclock = lists:keyfind("X-Riak-Vclock", 1, Headers),
            case do_put(Url, [State#state.client_id, Vclock], ValueGen) of
                ok ->
                    {ok, S2};
                {error, Reason} ->
                    {error, Reason, S2}
            end
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

next_url(State) when is_record(State#state.base_urls, url) ->
    {State#state.base_urls, State};
next_url(State) when State#state.base_urls_index > tuple_size(State#state.base_urls) ->
    { element(1, State#state.base_urls),
      State#state { base_urls_index = 1 } };
next_url(State) ->
    { element(State#state.base_urls_index, State#state.base_urls),
      State#state { base_urls_index = State#state.base_urls_index + 1 }}.

do_get(BaseUrl, KeyGen) ->
    Url = BaseUrl#url { path = lists:concat([BaseUrl#url.path, '/', KeyGen()]) },
    case send_request(Url, [], get, [], [{response_format, binary}]) of
        {ok, "404", _Headers, _Body} ->
            {not_found, Url};
        {ok, "300", Headers, _Body} ->
            {ok, Url, Headers};
        {ok, "200", Headers, _Body} ->
            {ok, Url, Headers};
        {ok, Code, _Headers, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            disconnect(Url),
            {error, Reason}
    end.

do_put(Url, Headers, ValueGen) ->
    case send_request(Url, Headers ++ [{'Content-Type', 'application/octet-stream'}],
                      put, ValueGen(), [{response_format, binary}]) of
        {ok, "204", _Header, _Body} ->
            Res = ok;
        {ok, Code, _Header, _Body} ->
            Res = {error, {http_error, Code}};
        {error, Reason} ->
            Res = {error, Reason}
    end,
    disconnect(Url),
    Res.

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


send_request(Url, Headers, Method, Body, Options) ->
    send_request(Url, Headers, Method, Body, Options, 3).

send_request(Url, Headers, Method, Body, Options, 0) ->
    {error, max_retries};
send_request(Url, Headers, Method, Body, Options, Count) ->
    Pid = connect(Url),
    case catch(ibrowse_http_client:send_req(Pid, Url, Headers, Method, Body, Options, 15000)) of
        {'EXIT', {timeout, _}} ->
            {error, req_timedout};
        {'EXIT', Reason} ->
            {error, {'EXIT', Reason}};
        {error, connection_closed} ->
            disconnect(Url),
            send_request(Url, Headers, Method, Body, Options);
        Other ->
            Other
    end.

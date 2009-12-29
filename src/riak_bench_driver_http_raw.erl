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

-export([new/0,
         run/4]).

-include("riak_bench.hrl").

-record(url, {abspath, host, port, username, password, path, protocol}).

-record(state, { pid,
                 base_url }).

%% ====================================================================
%% API
%% ====================================================================

new() ->
    %% Make sure ibrowse is available
    case code:which(ibrowse) of
        non_existing ->
            ?FAIL_MSG("~s requires ibrowse to be installed.\n", [?MODULE]);
        _ ->
            ok
    end,

    application:start(ibrowse),

    %% Get hostname/port we'll be testing
    {Hostname, Port} = riak_bench_config:get(http_raw_host, {"127.0.0.1", 8098}),

    %% Get the base URL
    BaseUrl = #url { host = Hostname,
                     port = Port,
                     path = riak_bench_config:get(http_raw_base, "/raw/test")},
    
    {ok, #state { base_url = BaseUrl}}.


run(get, KeyGen, _ValueGen, State) ->
    case do_get(State#state.base_url, KeyGen) of
        {not_found, _Url} ->
            {ok, State};
        {ok, _Url, _Headers} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    BaseUrl = State#state.base_url,
    Url = BaseUrl#url { path = lists:concat([BaseUrl#url.path, '/', KeyGen()]) },
    case do_put(Url, [], ValueGen) of
        ok ->
            disconnect(),
            {ok, State};
        {error, Reason} ->
            disconnect(),
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    case do_get(State#state.base_url, KeyGen) of
        {error, Reason} ->
            {error, Reason, State};

        {not_found, Url} ->
            case do_put(Url, [], ValueGen) of
                ok ->
                    disconnect(),
                    {ok, State};
                {error, Reason} ->
                    disconnect(),
                    {error, Reason}
            end;

        {ok, Url, Headers} ->
            Vclock = lists:keyfind("X-Riak-Vclock", 1, Headers),
            case do_put(Url, [Vclock], ValueGen) of
                ok ->
                    disconnect(),
                    {ok, State};
                {error, Reason} ->
                    disconnect(),
                    {error, Reason}
            end
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

do_get(BaseUrl, KeyGen) ->
    Url = BaseUrl#url { path = lists:concat([BaseUrl#url.path, '/', KeyGen()]) },
    case send_request(Url, [], get, [], [{response_format, binary}]) of
        {ok, "404", _Headers, _Body} ->
            {not_found, Url};
        {ok, "200", Headers, _Body} ->
            {ok, Url, Headers};
        {error, Reason} ->
            {error, Reason}
    end.

do_put(Url, Headers, ValueGen) ->
    case send_request(Url, Headers ++ [{'Content-Type', 'application/octet-stream'}],
                      put, ValueGen(), [{response_format, binary}]) of
        {ok, "204", _Header, _Body} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
            
connect() ->
    case erlang:get(ibrowse_pid) of
        undefined ->
            {Hostname, Port} = riak_bench_config:get(http_raw_host, {"127.0.0.1", 8098}),
            {ok, Pid} = ibrowse_http_client:start({Hostname, Port}),
            erlang:put(ibrowse_pid, Pid),
            Pid;
        Pid ->
            Pid
    end.


disconnect() ->
    case erlang:get(ibrowse_pid) of
        undefined ->
            ok;
        OldPid ->
            catch(ibrowse_http_client:stop(OldPid))
    end,
    erlang:erase(ibrowse_pid),
    ok.


send_request(Url, Headers, Method, Body, Options) ->
    Pid = connect(),
    case catch(ibrowse_http_client:send_req(Pid, Url, Headers, Method, Body, Options, 15000)) of    
        {'EXIT', {timeout, _}} ->
            {error, req_timedout};
        {'EXIT', Reason} ->
            {error, {'EXIT', Reason}};
        Other ->
            Other
    end.
              

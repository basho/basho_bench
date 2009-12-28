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

-record(state, { url }).

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
    ibrowse:start(),
                     
    {ok, #state { url = riak_bench_config:get(http_raw_url, "http://127.0.0.1:8098/raw/test")}}.


run(get, KeyGen, _ValueGen, State) ->
%    timer:sleep(100),
    case do_get(State, KeyGen) of
        {not_found, _Url} ->
            {ok, State};
        {ok, _Url, _Headers} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    timer:sleep(100),
    Url = lists:concat([State#state.url, '/', KeyGen()]),
    case do_put(Url, [], ValueGen) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

% run(update, KeyGen, ValueGen, State) ->
%     case do_get(State, KeyGen) of
%         {error, Reason} ->
%             {error, Reason, State};

%         {not_found, Url} ->
%             case do_put(Url, [], ValueGen) of
%                 ok ->
%                     {ok, State};
%                 {error, Reason} ->
%                     {error, Reason}
%             end;

%         {ok, Url, Headers} ->
%             Vclock = lists:keyfind("X-Riak-Vclock", 1, Headers),
%             case do_put(Url, [Vclock], ValueGen) of
%                 ok ->
%                     {ok, State};
%                 {error, Reason} ->
%                     {error, Reason}
%             end
%     end.


%% ====================================================================
%% Internal functions
%% ====================================================================

do_get(State, KeyGen) ->
    Url = lists:concat([State#state.url, '/', KeyGen()]),
    case ibrowse:send_req(Url, [], get, [], [{response_format, binary}]) of
        {ok, "404", _Headers, _Body} ->
            {not_found, Url};
        {ok, "200", Headers, _Body} ->
            {ok, Url, Headers};
        {ok, Any, _Header, _Body} ->
            io:format("GET ~p\n", [Any]),
            {ok, Url, []};
        {error, Reason} ->
            {error, Reason}
    end.

do_put(Url, Headers, ValueGen) ->
    case ibrowse:send_req(Url,
                          Headers ++ [{'Content-Type', 'application/octet-stream'}],
                          put, ValueGen(), [{response_format, binary}]) of
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Any, _Header, _Body} ->
            io:format("PUT ~p\n", [Any]),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
            

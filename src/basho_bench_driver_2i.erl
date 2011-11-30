%% -------------------------------------------------------------------
%%
%% basho_bench_driver_2i_pb: Driver for Secondary Indices (via PB)
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
-module(basho_bench_driver_2i).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {
          pb_pid,
          http_host,
          http_port,
          bucket
         }).


%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Ensure that inets is started...
    application:start(inets),

    %% Ensure that riakc library is in the path...
    ensure_module(riakc_pb_socket),
    ensure_module(mochijson2),

    %% Read config settings...
    PBIPs  = basho_bench_config:get(pb_ips, [{127,0,0,1}]),
    PBPort  = basho_bench_config:get(pb_port, 8087),
    HTTPHosts = basho_bench_config:get(http_hosts, ["127.0.0.1"]),
    HTTPPort =  basho_bench_config:get(http_port, 8098),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"mybucket">>),

    PBIP = choose(Id, PBIPs),
    case riakc_pb_socket:start_link(PBIP, PBPort) of
        {ok, Pid} ->
            {ok, #state {
               pb_pid = Pid,
               http_host = choose(Id, HTTPHosts),
               http_port = HTTPPort,
               bucket = Bucket }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p port ~p: ~p\n",
                      [PBIP, PBPort, Reason2])
    end.

%% Get a single object.
run(get_pb, KeyGen, _ValueGen, State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.bucket,
    Key = to_binary(KeyGen()),
    case riakc_pb_socket:get(Pid, Bucket, Key) of
        {ok, _Obj} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

%% Put an object with N indices.
run({put_pb, N}, KeyGen, ValueGen, State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.bucket,
    Key = to_integer(KeyGen()),
    Value = ValueGen(),
    Indexes = generate_integer_indexes_for_key(Key, N),
    MetaData = dict:from_list([{<<"index">>, Indexes}]),

    %% Create the object...
    Robj0 = riakc_obj:new(Bucket, to_binary(Key)),
    Robj1 = riakc_obj:update_value(Robj0, Value),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),

    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj2) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

%% Query results via the HTTP interface.
run({query_http, N}, KeyGen, _ValueGen, State) ->
    Host = State#state.http_host,
    Port = State#state.http_port,
    Bucket = State#state.bucket,
    StartKey = to_integer(KeyGen()),
    EndKey = StartKey + N - 1,
    URL = io_lib:format("http://~s:~p/buckets/~s/index/field1_int/~p/~p", 
                    [Host, Port, Bucket, StartKey, EndKey]),
    case json_get(URL) of
        {ok, {struct, Proplist}} ->
            case proplists:get_value(<<"keys">>, Proplist) of
                Results when length(Results) == N ->
                    {ok, State};
                Results ->
                    io:format("Not enough results for query_http: ~p~n", [Results]),
                    {ok, State}
            end;
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n", [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end;

%% Query results via the M/R interface.
run({query_mr, 1}, KeyGen, _ValueGen, State) ->
    Host = State#state.http_host,
    Port = State#state.http_port,
    Bucket = State#state.bucket,
    Key = to_integer(KeyGen()),
    URL = io_lib:format("http://~s:~p/mapred", [Host, Port]),
    Body = ["
      {
         \"inputs\":{
             \"bucket\":\"", to_list(Bucket), "\",
             \"index\":\"field1_int\",
             \"key\":\"", to_list(Key), "\"
         },
         \"query\":[
            {
               \"reduce\":{
                  \"language\":\"erlang\",
                  \"module\":\"riak_kv_mapreduce\",
                  \"function\":\"reduce_identity\",
                  \"keep\":true
               }
            }
         ]
      }
    "],
    case json_post(URL, Body) of
        {ok, Results} when length(Results) == 1 ->
            {ok, State};
        {ok, Results} ->
            io:format("Not enough results for query_mr: ~p~n", [Results]),
            {ok, State};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n", [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end;

run({query_mr, N}, KeyGen, _ValueGen, State) ->
    Host = State#state.http_host,
    Port = State#state.http_port,
    Bucket = State#state.bucket,
    StartKey = to_integer(KeyGen()),
    EndKey = StartKey + N - 1,
    URL = io_lib:format("http://~s:~p/mapred", [Host, Port]),
    Body = ["
      {
         \"inputs\":{
             \"bucket\":\"", to_list(Bucket), "\",
             \"index\":\"field1_int\",
             \"start\":\"",to_list(StartKey), "\",
             \"end\":\"", to_list(EndKey), "\"
         },
         \"query\":[
            {
               \"reduce\":{
                  \"language\":\"erlang\",
                  \"module\":\"riak_kv_mapreduce\",
                  \"function\":\"reduce_identity\",
                  \"keep\":true
               }
            }
         ]
      }
    "],
    case json_post(URL, Body) of
        {ok, Results} when length(Results) == N ->
            {ok, State};
        {ok, Results} ->
            io:format("Not enough results for query_mr: ~p~n", [Results]),
            {ok, State};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n", [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end;

%% Query results via the PB interface.
run({query_pb, N}, KeyGen, _ValueGen, State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.bucket,
    StartKey = to_integer(KeyGen()),
    EndKey = StartKey + N - 1,
    case riakc_pb_socket:get_index(Pid, Bucket, <<"field1_int">>,
                                   to_binary(StartKey), to_binary(EndKey)) of
        {ok, Results} when length(Results) == N ->
            {ok, State};
        {ok, Results} ->
            io:format("Not enough results for query_pb: ~p~n", [Results]),
            {ok, State};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n", [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end;

run(Other, _, _, _) ->
    throw({unknown_operation, Other}).

%% ====================================================================
%% Internal functions
%% ====================================================================

generate_integer_indexes_for_key(Key, N) ->
    F = fun(X) ->
                {"field" ++ to_list(X) ++ "_int", Key}
        end,
    [F(X) || X <- lists:seq(1, N)].

to_binary(B) when is_binary(B) ->
    B;
to_binary(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I));
to_binary(L) when is_list(L) ->
    list_to_binary(L).

to_integer(I) when is_integer(I) ->
    I;
to_integer(B) when is_binary(B) ->
    list_to_integer(binary_to_list(B));
to_integer(L) when is_list(L) ->
    list_to_integer(L).

to_list(L) when is_list(L) ->
    L;
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(I) when is_integer(I) ->
    integer_to_list(I).

choose(N, L) ->
    lists:nth((N rem length(L) + 1), L).

json_get(Url) ->
    Request = {lists:flatten(Url), []},
    case httpc:request(get, Request, [], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, mochijson2:decode(Body)};
        Other ->
            {error, Other}
    end.

json_post(Url, Payload) ->
    Request = {lists:flatten(Url), [], "application/json", lists:flatten(Payload)},
    case httpc:request(post, Request, [], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, mochijson2:decode(Body)};
        Other ->
            {error, Other}
    end.

ensure_module(Module) ->
    case code:which(Module) of
        non_existing ->
            ?FAIL_MSG("~s requires " ++ atom_to_list(Module) ++ " module to be available on code path.\n", [?MODULE]);
        _ ->
            ok
    end.

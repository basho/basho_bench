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
-module(basho_bench_driver_simple_with_https).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { client_id, host, 
                 users, user_index, 
                 policy_ids, policy_index,
                 group_names, group_index}).

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
    case ssl:start() of
        ok ->
            ok;
        {error, {already_started, ssl}} ->
            ok;
        _ ->
            ?FAIL_MSG("Unable to enable SSL support.\n", [])
    end,
    application:start(ibrowse),
    erlang:put(disconnect_freq, infinity),

    Host = basho_bench_config:get(host, "https://www.google.com"),
    UserTokens = basho_bench_config:get(user_tokens, []),
    PolicyIds = basho_bench_config:get(policy_ids, []),
    GroupNames = basho_bench_config:get(group_names, []),

    {ok, #state { client_id = Id,
                  host = Host,
                  users = setup_users(Host, UserTokens, 1, []),
                  user_index = 1,
                  policy_ids = PolicyIds,
                  policy_index = 1,
                  group_names = GroupNames,
                  group_index = 1}}.

setup_users(_Host, [], _Index, Users) ->
    Users;
setup_users(Host, [T|Rest], Index, Users) ->
    % Get device token
    Url = Host ++ "/api/dev",
    Headers = [{"Authorization","Basic " ++ T},{'Content-Type', 'application/json'}],
    Method = post,
    Payload = lists:flatten("{\"deviceName\": \"API_Test_Device\", \"pushToken\": \"APITestToken\", \"os\": \"WINDOWS\", \"clientDeviceId\": \"APITestDeviceId" ++ integer_to_list(Index) ++ "\", \"osVersion\": \"7.0\", \"type\": \"WINDOWS\"}"),
    
    {ok,"200",_Headers, Json} = ibrowse:send_req(Url, Headers, Method, Payload),
    {struct,[{<<"deviceId">>,DeviceId},{<<"deviceToken">>, DeviceToken},{<<"canProtect">>,true}]} = mochijson2:decode(Json),

    DeviceHeader = binary_to_list(base64:encode(binary_to_list(DeviceId) ++ ":" ++ binary_to_list(DeviceToken))),
    setup_users(Host, Rest, Index+1, [{Index, T, DeviceHeader}|Users]).

next_in_list([], _Itr, _Ind) ->
    error;
next_in_list([P|_], Ind, Ind) ->
    P;
next_in_list([_P|R], Itr, Ind) ->
    next_in_list(R, Itr+1, Ind).

next_device_token(State) ->
    Users = State#state.users,
    CurrentIndex = State#state.user_index,
    Header = find_user_with_index(Users, CurrentIndex),
    {Header, State#state {user_index = incr_index(CurrentIndex, length(Users))}}.

incr_index(T, T) -> 1;
incr_index(C, _) -> C + 1.

find_user_with_index([], _) ->
    error;
find_user_with_index([{Index, _, Header}|_], Index) ->
    Header;
find_user_with_index([_|Rest], Index) ->
    find_user_with_index(Rest, Index).

run({get, Path}, KeyGen, _ValueGen, State) ->
    KeyGen(),

    {Header, State2} = next_device_token(State),

    Url = State2#state.host ++ Path,
    Headers = [{"Authorization","Basic " ++ Header}],
    Method = get,

    case ibrowse:send_req(Url, Headers, Method) of
        {ok,"200", _, _} -> {ok, State2};
        Reason -> {error, Reason, State2}
    end;

run(sync_docs, KeyGen, _ValueGen, State) ->
    KeyGen(),

    {Header, State2} = next_device_token(State),

    Timestamp = timestamp(now()),

    Url = State2#state.host ++ "/api/doc/sync?syncId=" ++ Timestamp,
    Headers = [{"Authorization","Basic " ++ Header}],
    Method = get,

    case ibrowse:send_req(Url, Headers, Method) of
        {ok,"200", _, _} -> {ok, State2};
        Reason -> {error, Reason, State2}
    end;

run(protect_doc, KeyGen, _ValueGen, State) ->
    RandomString = KeyGen(),
    GroupName = next_in_list(State#state.group_names, 1, State#state.group_index),
    PolicyId = next_in_list(State#state.policy_ids, 1, State#state.policy_index),
    Payload = lists:flatten("{\"docName\": \"Default_Document.pdf\", \"docHash\": \"" ++ RandomString ++ "\", \"accessMap\" : {\"" ++ GroupName ++ "\" : \"" ++ PolicyId ++ "\"}}"),

    {Header, State2} = next_device_token(State),

    Url = State2#state.host ++ "/api/doc",
    Headers = [{"Authorization","Basic " ++ Header}, {'Content-Type', 'application/json'}],
    Method = post,

    State3 = State2#state {policy_index = incr_index(State#state.policy_index, length(State#state.policy_ids))},
    State4 = State3#state {group_index = incr_index(State#state.group_index, length(State#state.group_names))},

    case ibrowse:send_req(Url, Headers, Method, Payload) of
        {ok,"200", _, _} -> {ok, State4};
        Reason -> {error, Reason, State4}
    end.

timestamp(Now) -> 
    {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(Now), 
    lists:flatten(io_lib:format("~2..0w/~2..0w/~4..0w%20~2..0w:~2..0w:~2..0w", 
                  [MM, DD, YY, Hour, Min, Sec])).
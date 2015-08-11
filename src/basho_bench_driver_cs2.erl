%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2015 Basho Techonologies
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
-module(basho_bench_driver_cs2).

%% A basho_bench driver for load generating Riak CS, of second type.
%% First type driver (`basho_bench_driver_cs') is good at load
%% generating with large HTTP payload. It has measurament option that
%% offer data transfer throughput like MB/sec.  On the other hand,
%% this driver's purpose is for small objects.  If the first type is
%% used for small objects, user and bucket objects can be hot keys in
%% Riak. This one can handle multiple users and buckets to avoid it.

%% TODO
%% This driver requires Riak CS is configured as {admin_auth_enabled, false},
%% in order to automatically create users and buckets, also list users.
%% This can be avoided, for example, by reading local file of user list
%% create by `riak_cs_inspector'.

-export([new/1,
         run/4]).

-include("basho_bench.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").

-record(user, {
          index        :: pos_integer(),
          display_name :: binary(),
          key_id       :: binary(),
          key_secret   :: binary(),
          bucket       :: binary()
         }).

-record(state, {
          user_count        :: pos_integer(),
          host_base         :: binary(),
          target_urls,          % Tuple of #url{} -- one for each IP
          target_urls_index,    % #url{} to use for next request
          append_headers    :: [{atom() | string(), atom() | string()}]
         }).

-define(TAB, cs2_users).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    Disconnect = basho_bench_config:get(cs2_disconnect_frequency, infinity),
    case Disconnect of
        infinity -> ok;
        Seconds when is_integer(Seconds) -> ok;
        {ops, Ops} when is_integer(Ops) -> ok;
        _ -> ?FAIL_MSG("Invalid configuration for cs2_disconnect_frequency: ~p~n", [Disconnect])
    end,
    erlang:put(disconnect_freq, Disconnect),

    %% The IPs, port and path we'll be testing
    Ips  = basho_bench_config:get(cs2_hosts, ["127.0.0.1"]),
    DefaultPort = basho_bench_config:get(cs2_port, 8080),
    %% Convert the list to a tuple so we can efficiently round-robin
    %% through them.
    Targets = basho_bench_config:normalize_ips(Ips, DefaultPort),
    BaseUrls = list_to_tuple([#url{host=IP, port=Port}
                              || {IP, Port} <- Targets]),
    BaseUrlsIndex = random:uniform(tuple_size(BaseUrls)),

    UserCount = basho_bench_config:get(cs2_user_count, 5),
    HostBase  = list_to_binary(basho_bench_config:get(cs2_host_base, "s3.amazonaws.com")),
    AppendHeaders = basho_bench_config:get(cs2_append_headers, []),

    case Id of
        1 -> global_setup(element(1, BaseUrls), HostBase, UserCount);
        _ -> wait_for_setup(Id)
    end,
    {ok, #state{user_count=UserCount,
                host_base = HostBase,
                target_urls = BaseUrls,
                target_urls_index = BaseUrlsIndex,
                append_headers = AppendHeaders}}.

run(get, KeyGen, _ValueGen,
    #state{host_base=HostBase, user_count=UserCount} = State) ->
    Key = KeyGen(),
    {NextUrl, NewState} = next_url(State),
    {User, ObjectKey} = decompose_key(UserCount, Key),
    case do_get(NextUrl, ObjectKey, HostBase, User, State) of
        {ok, _Url, _Headers, _Body} ->
            {ok, NewState};
        {not_found, _Url} ->
            {ok, NewState};
        {error, Reason} ->
            {error, Reason, NewState}
    end;
run(get_existing, KeyGen, _ValueGen,
    #state{host_base=HostBase, user_count=UserCount} = State) ->
    Key = KeyGen(),
    {NextUrl, NewState} = next_url(State),
    {User, ObjectKey} = decompose_key(UserCount, Key),
    case do_get(NextUrl, ObjectKey, HostBase, User, State) of
        {ok, _Url, _Headers, _Body} ->
            {ok, NewState};
        {not_found, Url} ->
            {error, {not_found, Url}, NewState};
        {error, Reason} ->
            {error, Reason, NewState}
    end;
run(put, KeyGen, ValueGen,
    #state{host_base=HostBase, user_count=UserCount} = State) ->
    Key = KeyGen(),
    {NextUrl, NewState} = next_url(State),
    {User, ObjectKey} = decompose_key(UserCount, Key),
    case do_put(NextUrl, ObjectKey, ValueGen, HostBase, User, State) of
        ok ->
            {ok, NewState};
        {error, Reason} ->
            {error, Reason, NewState}
    end;
run(delete, KeyGen, _ValueGen,
    #state{user_count=UserCount, host_base=HostBase} = State) ->
    Key = KeyGen(),
    {NextUrl, NewState} = next_url(State),
    {User, ObjectKey} = decompose_key(UserCount, Key),
    case do_delete(NextUrl, ObjectKey, HostBase, User, State) of
        ok ->
            {ok, NewState};
        {error, Reason} ->
            {error, Reason, NewState}
    end;
run(put_delete, KeyGen, ValueGen,
    #state{host_base=HostBase, user_count=UserCount} = State) ->
    Key = KeyGen(),
    {NextUrl, NewState} = next_url(State),
    {User, ObjectKey} = decompose_key(UserCount, Key),
    case do_put(NextUrl, ObjectKey, ValueGen, HostBase, User, State) of
        ok ->
            case do_delete(NextUrl, ObjectKey, HostBase, User, State) of
                ok ->
                    {ok, NewState};
                {error, Reason} ->
                    {error, Reason, NewState}
            end;
        {error, Reason} ->
            {error, Reason, NewState}
    end;
run(noop, KeyGen, _ValueGen, #state{user_count=UserCount} = State) ->
    Key = KeyGen(),
    {_NextUrl, NewState} = next_url(State),
    {_User, _ObjectKey} = decompose_key(UserCount, Key),
    {ok, NewState}.

%% ====================================================================
%% Internal functions
%% ====================================================================

global_setup(Url, HostBase, UserCount) ->
    case ets:info(?TAB) of
        undefined -> global_setup0(Url, HostBase, UserCount);
        _         -> ok
    end.

global_setup0(Url, HostBase, UserCount) ->
    %% Make sure ibrowse is available
    case code:which(ibrowse) of
        non_existing ->
            ?FAIL_MSG("~s requires ibrowse to be installed.\n", [?MODULE]);
        _ ->
            ok
    end,
    case basho_bench_config:get(cs2_use_ssl, false) of
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
    application:start(ibrowse),
    case basho_bench_config:get(operations, []) of
        [{get_noop, 1}] ->
            ok;
        _ ->
            Self = self(),
            spawn(fun() ->
                          erlang:process_flag(trap_exit, true),
                          setup_users(Url, HostBase, UserCount, Self)
                  end),
            receive
                setup_finish ->
                    ?INFO("Finished global setup", []),
                    basho_bench_config:set(setup_finish, true)
            end
    end.

setup_users(Url, HostBase, UserCount, From) ->
    UserPrefix = basho_bench_config:get(cs2_user_prefix, "bb-user"),
    BucketPrefix = basho_bench_config:get(cs2_bucket_prefix, "bb-test"),
    ok = prepare_users(Url, UserPrefix, UserCount),
    AllUsers = list_users(Url),
    Users = filter_users(AllUsers, UserPrefix, BucketPrefix, UserCount, []),
    [maybe_create_bucket(Url, HostBase, User) ||
        User <- Users],
    ets:new(?TAB, [public, named_table,
                   {keypos, #user.index},
                   {read_concurrency, true}]),
    [ets:insert(?TAB, User) || User <- Users],
    From ! setup_finish,
    receive
        wait_forever_to_keep_ets_table_alive -> ok
    end.

prepare_users(_Url, _UserPrefix, 0) ->
    ok;
prepare_users(Url, UserPrefix, UserCount) ->
    ok = maybe_create_user(Url, UserPrefix, UserCount),
    prepare_users(Url, UserPrefix, UserCount - 1).

maybe_create_user(BaseUrl, UserPrefix, UserIndex) ->
    _DisplayName = UserPrefix ++ integer_to_list(UserIndex),
    Json = io_lib:format("{\"email\": \"~s~B@example.com\", \"name\": \"~s~B\"}",
                         [UserPrefix, UserIndex, UserPrefix, UserIndex]),
    Url = BaseUrl#url{path = "/riak-cs/user"},
    case send_request0(Url, post,
                      [{'Content-Type', 'application/json'}],
                      Json, [{response_format, binary}]) of
        {ok, "201", _Header, _Body} ->
            %% 201 Created
            ok;
        {ok, "409", _Header, _Body} ->
            %% 409 Conflict (User Already Exists)
            ok;
        {ok, Code, Header, Body} ->
            throw({error, {user_creation, Code, Header, Body}});
        {error, Reason} ->
            throw({error, {user_creation, Reason}})
    end.

list_users(BaseUrl) ->
    Url = BaseUrl#url{path = "/riak-cs/users"},
    case send_request0(Url, get, [{'Accept', 'application/json'}],
                       [], [{response_format, list}]) of
        {ok, "200", _Headers, Body} ->
            Users = parse_user_info(Body),
            Users;
        {ok, Code, Header, Body} ->
            throw({error, {user_creation, Code, Header, Body}});
        {error, Reason} ->
            lager:error("List users: ~p~n", [Reason]),
            throw({error, {user_creation, Reason}})
    end.

parse_user_info(Output) ->
    [Boundary | Tokens] = string:tokens(Output, "\r\n"),
    parse_user_info(Tokens, Boundary, []).

parse_user_info([_LastToken], _, Users) ->
    ordsets:from_list(Users);
parse_user_info(["Content-Type: application/json", RawJson | RestTokens],
                 Boundary, Users) ->
    UpdUsers = parse_user_records(RawJson, json) ++ Users,
    parse_user_info(RestTokens, Boundary, UpdUsers);
parse_user_info([_ | RestTokens], Boundary, Users) ->
    parse_user_info(RestTokens, Boundary, Users).

parse_user_records(Output, json) ->
    JsonData = mochijson2:decode(Output),
    [begin
         KeyId = binary_to_list(proplists:get_value(<<"key_id">>, UserJson)),
         KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, UserJson)),
         Name = binary_to_list(proplists:get_value(<<"name">>, UserJson)),
         {Name, KeyId, KeySecret}
     end || {struct, UserJson} <- JsonData].

filter_users(_Users, _UserPrefix, _BucketPrefix, 0, Acc) ->
    Acc;
filter_users(Users, UserPrefix, BucketPrefix, UserIndex, Acc) ->
    DisplayName = UserPrefix ++ integer_to_list(UserIndex),
    {Name, KeyId, KeySecret} = lists:keyfind(DisplayName, 1, Users),
    case lists:prefix(UserPrefix, Name) of
        false ->
            filter_users(Users, UserPrefix, BucketPrefix, UserIndex - 1, Acc);
        true ->
            User = #user{index=UserIndex,
                         key_id=list_to_binary(KeyId),
                         key_secret=list_to_binary(KeySecret),
                         display_name=list_to_binary(Name),
                         bucket=list_to_binary(BucketPrefix ++
                                                   integer_to_list(UserIndex))},
            filter_users(Users, UserPrefix, BucketPrefix, UserIndex - 1, [User | Acc])
    end.

maybe_create_bucket(BaseUrl, HostBase, User) ->
    Url = BaseUrl#url{path="/"},
    case send_request(Url, HostBase, put, [], [], User) of
        {ok, "200", _Headers, _Body} ->
            ok;
        %% {ok, "409", _Headers, _Body} ->
        %%     ok;
        {ok, Code, Header, Body} ->
            lager:error("Create bucket: ~p~n", [{Code, Header, Body}]),
            throw({error, {bucket_creation, Code, Header, Body}});
        {error, Reason} ->
            lager:error("Create bucket: ~p~n", [Reason]),
            throw({error, {bucket_creation, Reason}})
    end.

wait_for_setup(Id) ->
    case basho_bench_config:get(setup_finish, false) of
        false ->
            ?DEBUG("Waiting global setup finished: worker ID = ~p", [Id]),
            timer:sleep(500),
            wait_for_setup(Id);
        true ->
            ok
    end.

auth_sig(AccessKey, SecretKey, Method, ContentType, Date, Headers, Resource) ->
    AmzHeaders = lists:filter(fun ({"x-amz-" ++ _, V}) when V =/= undefined -> true; (_) -> false end, Headers),
    CanonizedAmzHeaders =
        [[Name, $:, Value, $\n] || {Name, Value} <- lists:sort(AmzHeaders)],
    StringToSign = [uppercase_verb(Method), $\n,
                    "", $\n,  % Content-MD5
                    ContentType, $\n,
                    Date, $\n,
                    CanonizedAmzHeaders,
                    Resource
                   ],
    Signature = base64:encode(sha_mac(SecretKey, StringToSign)),
    ["AWS ", AccessKey, $:, Signature].

sha_mac(KeyData, STS) ->
    crypto:hmac(sha, KeyData, STS).

uppercase_verb(put) ->
    <<"PUT">>;
uppercase_verb(get) ->
    <<"GET">>;
uppercase_verb(delete) ->
    <<"DELETE">>.

host(HostBase, BucketName) ->
    binary_to_list(<<BucketName/binary, $., HostBase/binary>>).

next_url(State) when State#state.target_urls_index > tuple_size(State#state.target_urls) ->
    next_url(State#state { target_urls_index = 1 });
next_url(State) ->
    { element(State#state.target_urls_index, State#state.target_urls),
      State#state { target_urls_index = State#state.target_urls_index + 1 }}.

%% divmod decomposer, but 1-origin.
%% If `UserCount' is 3, output keys are as follows.
%% | input | user key | object key |
%% |     1 |        1 |          1 |
%% |     2 |        2 |          1 |
%% |     3 |        3 |          1 |
%% |     4 |        1 |          2 |
%% |     5 |        2 |          2 |
-spec decompose_key(pos_integer(), non_neg_integer()) ->
                           {#user{}, string()}.
decompose_key(UserCount, Key) ->
    UserKey = (Key rem UserCount) + 1,
    [User] = ets:lookup(?TAB, UserKey),
    ObjectKey = (Key div UserCount) + 1,
    {User, integer_to_list(ObjectKey)}.

do_get(BaseUrl, Key, HostBase, User, State) ->
    Url = BaseUrl#url{path = "/" ++ Key},
    case send_request(Url, HostBase, get, State#state.append_headers, [], User) of
        {ok, "200", Headers, Body} ->    % 200 OK
            {ok, Url, Headers, Body};
        {ok, "206", Headers, Body} ->    % 206 Partial Content
            {ok, Url, Headers, Body};
        {ok, "300", Headers, _Body} ->   % 300 Multiple Choices
            {multiple_choices, Url, Headers};
        {ok, "404", _Headers, _Body} ->  % 404 Not Found
            {not_found, Url};
        {ok, Code, _Headers, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_put(BaseUrl, Key, ValueGen, HostBase, User, State) ->
    Val = if is_function(ValueGen) ->
                  ValueGen();
             true ->
                  ValueGen
          end,
    Url = BaseUrl#url{path = "/" ++ Key},
    case send_request(Url, HostBase, put, State#state.append_headers, Val, User) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete(BaseUrl, Key, HostBase, User, State) ->
    Url = BaseUrl#url{path = "/" ++ Key},
    case send_request(Url, HostBase, delete, State#state.append_headers, [], User) of
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
    Key = {ibrowse_pid, Url#url.host, Url#url.port},
    case erlang:get(Key) of
        undefined ->
            {ok, Pid} = ibrowse_http_client:start({Url#url.host, Url#url.port}),
            erlang:put(Key, Pid),
            Pid;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    erlang:erase(Key),
                    connect(Url)
            end
    end.

disconnect(Url) ->
    Key = {ibrowse_pid, Url#url.host, Url#url.port},
    case erlang:get(Key) of
        undefined ->
            ok;
        OldPid ->
            catch(ibrowse_http_client:stop(OldPid))
    end,
    erlang:erase(Key),
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
        _Seconds -> erlang:put({last_disconnect, Url#url.host}, os:timestamp())
    end.

send_request(Url, HostBase, Method, Headers, Body, User) ->
    send_request(Url, HostBase, Method, Headers, Body, User, []).

send_request(Url, HostBase, Method, Headers0, Body,
             #user{key_id=KeyId, key_secret=KeySecret, bucket=BucketName}, Options) ->
    Date = httpd_util:rfc1123_date(),
    {ContentType, Headers1} =
        case lists:keyfind('Content-Type', 1, Headers0) of
            false -> {"application/octet-stream",
                      [{'Content-Type', 'application/octet-stream'} | Headers0]};
            {_, CT} -> {CT, Headers0}
        end,
    Headers = [{'Date', Date},
               {'Host', host(HostBase, BucketName)} | Headers1],
    Resource = [$/, BucketName, Url#url.path],
    AuthStr = auth_sig(KeyId, KeySecret, Method,
                       ContentType, Date, Headers, Resource),
    HeadersWithAuth = [{'Authorization', AuthStr} | Headers],
    send_request0(Url, Method, HeadersWithAuth, Body,
                  [{headers_as_is, true}, {response_format, binary} | Options]).

send_request0(Url, Method, Headers, Body, Options) ->
    Pid = connect(Url),
    Options2 = case basho_bench_config:get(cs2_use_ssl, false) of
                   false ->
                       Options;
                   true ->
                       [{is_ssl, true}, {ssl_options, []} | Options];
                   SSLOpts when is_list(SSLOpts) ->
                       [{is_ssl, true}, {ssl_options, SSLOpts} | Options]
               end,
    case catch(ibrowse_http_client:send_req(
                 Pid, Url,
                 Headers ++ basho_bench_config:get(cs2_append_headers,[]),
                 Method, Body, Options2,
                 basho_bench_config:get(cs2_request_timeout, 5000))) of
        {ok, Status, RespHeaders, RespBody} ->
            maybe_disconnect(Url),
            {ok, Status, RespHeaders, RespBody};

        Error ->
            clear_disconnect_freq(Url),
            disconnect(Url),
            normalize_error(Method, Error)
    end.

normalize_error(Method, {'EXIT', {timeout, _}})  -> {error, {Method, timeout}};
normalize_error(Method, {'EXIT', Reason})        -> {error, {Method, 'EXIT', Reason}};
normalize_error(Method, {error, Reason})         -> {error, {Method, Reason}}.

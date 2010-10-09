%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2010 Scott Lystig Fritchie, <slfritchie@snookles.com>
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
-module(basho_bench_driver_hibari).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

%% UBF string helper
-define(S(X), #'#S'{value=X}).

%% UBF string record
-record('#S', {value=""}).

-define(BASIC_TIMEOUT, 10*1000).

-record(state, {
          hibari_type,
          id,
          hibari_servers,
          hibari_port,
          hibari_service,
          clnt,
          table,
          value_generator
         }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Try to tell the user politely if there's a missing module and where
    %% that module might be found.
    DepMods = [
               {brick_simple,
                "/path/to/your-hibari-distro/src/erl-apps/gdss__HEAD/ebin"},
               {gmt_util,
                "/path/to/your-hibari-distro/src/erl-apps/gmt-util__HEAD/ebin"},
               {ubf_gdss_plugin,
                "/path/to/your-hibari-distro/src/erl-apps/gdss-ubf-proto__HEAD/ebin"},
               {cluster_info,
                "/path/to/your-hibari-distro/src/erl-apps/cluster-info__HEAD/ebin"},
               {jsf,
                "/path/to/your-hibari-distro/src/erl-tools/ubf-jsonrpc__HEAD/ebin"},
               {tbf,
                "/path/to/your-hibari-distro/src/erl-tools/ubf-thrift__HEAD/ebin"},
               {mochijson2,
                "/path/to/your-hibari-distro/src/erl-third-party/mochiweb__HEAD/ebin"},
               {ubf_client,
                "/path/to/your-hibari-distro/src/erl-tools/ubf__HEAD/ebin"}
              ] ++
        [{Mod, x} || Mod <- [
                             %% gdss app
                             brick_simple, brick_server, brick_hash,
                             %% gmt app
                             gmt_util,
                             %% ubf app
                             contract_driver, ubf, ubf_client, ubf_driver
                            ]],
    if Id == 1 ->
            F = fun({Mod, Dir}) ->
                        case code:load_file(Mod) of
                            {module, Mod} ->
                                ok;
                            {error, not_purged} ->
                                %% This is OK: generator #1 crashed & restarted.
                                ok;
                            _Load ->
                                ?ERROR("~p: error loading '~p' module: ~p.\n\n",
                                       [?MODULE, Mod, _Load]),
                                if is_list(Dir) ->
                                        ?FAIL_MSG(
                                           "Please double-check the path for "
                                           "this module in\nthe basho_bench "
                                           "config file, e.g.\n    ~s\n",
                                           [Dir]);
                                   true ->
                                        ok
                                end
                        end
                end,
            lists:map(F, DepMods),
            ?INFO("All required modules are available.\n", []);
       true ->
            ok
    end,

    Table = basho_bench_config:get(hibari_table),
    HibariServers = basho_bench_config:get(hibari_servers),
    HibariType = basho_bench_config:get(hibari_client_type),
    HibariTcpPort = basho_bench_config:get(hibari_server_tcp_port),
    HibariService = basho_bench_config:get(hibari_server_service),
    Native1Node = basho_bench_config:get(hibari_native_1node),
    NativeSname = basho_bench_config:get(hibari_native_my_sname),
    NativeTickTime = basho_bench_config:get(hibari_native_ticktime),
    NativeCookie = basho_bench_config:get(hibari_native_cookie),
    ValueGenerator = basho_bench_config:get(value_generator),

    Clnt = make_clnt(HibariType, Id, NativeSname, NativeTickTime, NativeCookie,
                     Native1Node, HibariServers, HibariTcpPort, HibariService, ValueGenerator, false),

    {ok, #state{hibari_type = HibariType,
                id = Id,
                hibari_servers = HibariServers,
                hibari_port = HibariTcpPort,
                hibari_service = HibariService,
                clnt = Clnt,
                table = Table,
                value_generator = ValueGenerator
               }}.

run(get, KeyGen, _ValueGen, #state{hibari_type = ClientType, clnt = Clnt,
                                   table = Table} = S) ->
    Key = KeyGen(),
    case do(ClientType, Clnt, Table, [brick_server:make_get(Key)]) of
        [{ok, _TS, _Val}] ->
            {ok, S};
        [key_not_exist] ->
            {ok, S};
        timeout ->
            {error, timeout, close_and_reopen(S)};
        _Else ->
            {'EXIT', _Else}
    end;
run(put, KeyGen, ValueGen, #state{hibari_type = ClientType, clnt = Clnt,
                                  table = Table} = S) ->
    Key = KeyGen(),
    Val = ValueGen(),
    case do(ClientType, Clnt, Table, [brick_server:make_set(Key, Val)]) of
        [ok] ->
            {ok, S};
        [{ts_error, _TS}] ->
            %% Honest race with another put.
            {ok, S};
        timeout ->
            {error, timeout, close_and_reopen(S)};
        _Else ->
            {'EXIT', _Else}
    end;
run(delete, KeyGen, _ValueGen, #state{hibari_type = ClientType, clnt = Clnt,
                                      table = Table} = S) ->
    Key = KeyGen(),
    case do(ClientType, Clnt, Table, [brick_server:make_delete(Key)]) of
        [ok] ->
            {ok, S};
        [key_not_exist] ->
            {ok, S};
        timeout ->
            {error, timeout, close_and_reopen(S)};
        _Else ->
            {'EXIT', _Else}
    end.

%%%
%%% Private funcs
%%%

do(native, _Clnt, Table, OpList) ->
    try
        brick_simple:do(Table, OpList, ?BASIC_TIMEOUT)
    catch _X:_Y ->
        ?ERROR("Error on ~p: ~p ~p\n", [Table, _X, _Y]),
        {error, {_X, _Y}}
    end;
do(Type, Clnt, Table, OpList)
  when Type == ubf; Type == ebf; Type == jsf; Type == tbf ->
    try begin
            DoOp = {do, Table, OpList, [], ?BASIC_TIMEOUT+100},
            {reply, Res, none} = ubf_client:rpc(Clnt, DoOp, ?BASIC_TIMEOUT),
            Res
        end
    catch
        error:{badmatch, {error, socket_closed}} ->
            socket_closed;
        error:{badmatch, timeout} ->
            timeout;
        error:badpid ->
            %% This error corresponds to a timeout error .....
            ?ERROR("TODO: error:badpid for ~p\n", [Clnt]),
            error_badpid;
        _X:_Y ->
            ?ERROR("Error on ~p: ~p ~p\n", [Table, _X, _Y]),
            bummer
    end.

make_clnt(HibariType, Id, NativeSname, NativeTickTime, NativeCookie,
          Native1Node, HibariServers, HibariTcpPort, HibariService, ValueGenerator, RetryP) ->
    case HibariType of
        native when Id == 1 ->
            ?INFO("Try to start net_kernel with name ~p\n", [NativeSname]),
            case net_kernel:start([NativeSname, shortnames, 1000*NativeTickTime]) of
                {ok, _} ->
                    ?INFO("Net kernel started as ~p\n", [node()]);
                {error, {already_started, _}} ->
                    ok;
                {error, Reason} ->
                    ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
            end,

            ?INFO("Set cookie to ~p\n", [NativeCookie]),
            true = erlang:set_cookie(node(), NativeCookie),

            ?INFO("Try to ping ~p\n", [Native1Node]),
            case net_adm:ping(Native1Node) of
                pong ->
                    ok;
                pang ->
                    ?FAIL_MSG("~s: cannot ping ~p, aborting!\n",
                              [?MODULE, Native1Node])
            end,
            application:start(sasl),
            application:start(crypto),
            EmptyPath = "./empty_file",
            ok = file:write_file(EmptyPath, <<>>),
            application:set_env(gmt, central_config, EmptyPath),
            ok = application:start(gmt),
            ok = application:start(gdss_client),
            timer:sleep(2000),
            ?INFO("Hibari client app started\n", []),
            undefined;
        native ->
            %% All the work was done above in Id == 1 clause
            undefined;
        X when X == ebf; X == ubf; X == jsf; X == tbf ->
            %% Choose the node using our ID as a modulus
            Server = lists:nth((Id rem length(HibariServers)+1),
                               HibariServers),
            if not RetryP -> ?INFO("Using server ~p for ~p worker ~p\n",
                                   [Server, HibariType, Id]);
               true       -> ok
            end,
            case ubf_client:connect(Server, HibariTcpPort,
                                    [{proto, HibariType}], 60*1000) of
                {ok, Pid, _} ->
                    Args =
                        case HibariService of
                            gdss_stub ->
                                case ValueGenerator of
                                    %% extract and set value size to be used for gdss stub's get responses
                                    {value_generator, {fixed_bin, ValueSize}} when is_integer(ValueSize) ->
                                        [{get_value_size, ValueSize}];
                                    _ ->
                                        []
                                end;
                            _ ->
                                []
                        end,
                    {reply, {ok, ok}, none} =
                        ubf_client:rpc(Pid, {startSession, ?S(atom_to_list(HibariService)), Args},
                                       60*1000),
                    Pid;
                Error ->
                    ?FAIL_MSG("~s: id ~p cannot connect to "
                              "~p port ~p: ~p\n",
                              [?MODULE, Id, Server, HibariTcpPort, Error])
            end
    end.

%% UBF/EBF/JSF/TBF timeouts cause problems for basho_bench.  If we
%% report an error, we'll timeout.  But if we don't report an error,
%% then basho_bench will assume that we can continue using the same
%% #state and therefore the same UBF/EBF/JSF/TBF client ... but that
%% client is now in an undefined state and cannot be used again.
%%
%% So, we use this function to forcibly close the current client and
%% open a new one.  For the native client, this ends up doing almost
%% exactly nothing.

close_and_reopen(#state{clnt = OldClnt,
                        id = OldId,
                        hibari_type = HibariType,
                        hibari_servers = HibariServers,
                        hibari_port = HibariTcpPort,
                        hibari_service = HibariService,
                        value_generator = ValueGenerator} = S) ->
    %%?INFO("close_and_reopen #~p\n", [OldId]),
    catch ubf_client:close(OldClnt),

    %% Take advantage of the fact that native clients don't need to
    %% re-do anything, so just pass 'foo' instead.
    Id = if HibariType == native -> foo;
            true                 -> OldId
         end,
    NewClnt = make_clnt(HibariType, Id, foo, foo, foo,
                        foo, HibariServers, HibariTcpPort, HibariService, ValueGenerator, true),
    S#state{clnt = NewClnt}.

%% -------------------------------------------------------------------
%%
%% basho_bench_driver_2i_nhs: Driver for NHS-like workloads
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
-module(basho_bench_driver_nhs).

-export([new/1,
         run/4]).

-export([run_aaequery/1,
            run_listkeys/1,
            run_segmentfold/1]).

-include("basho_bench.hrl").

-define(QUERYLOG_FREQ, 2000).
-define(FORCEAAE_FREQ, 10). % Every 10 seconds

-record(state, {
                pb_pid,
                repl_pid,
                http_host,
                http_port,
                recordBucket,
                documentBucket,
                pb_timeout,
                http_timeout,
                fold_timeout,
                alwaysget_perworker_maxkeycount = 1 :: pos_integer(),
                alwaysget_perworker_minkeycount = 1 :: pos_integer(),
                alwaysget_keyorder :: key_order|skew_order,
                unique_size :: pos_integer(),
                unique_keyorder :: key_order|skew_order,
                postcode_indexcount = 3 :: pos_integer(),
                postcodeq_count = rand:uniform(?QUERYLOG_FREQ)
                    :: non_neg_integer(),
                dobq_count = rand:uniform(?QUERYLOG_FREQ)
                    :: non_neg_integer(),
                query_logfreq :: pos_integer(),
                nominated_id :: boolean(),
                % ID 1 is nominated to do special work
                singleton_pid :: pid() | undefined,
                unique_key_count = 1 :: non_neg_integer(),
                unique_key_lowcount = 1 :: non_neg_integer(),
                alwaysget_key_count = 1 :: non_neg_integer(),
                keyid :: binary(),
                last_forceaae = os:timestamp() :: erlang:timestamp()
         }).


-define(POSTCODE_AREAS,
                [{1, "AB"}, {2, "AL"}, {3, "B"}, {4, "BA"}, {5, "BB"}, 
                {6, "BD"}, {7, "BH"}, {8, "BL"}, {9, "BN"}, {10, "BR"}, 
                {11, "BS"}, {12, "BT"}, {13, "CA"}, {14, "CB"}, {15, "CF"}, 
                {16, "CH"}, {17, "CM"}, {18, "CO"}, {19, "CR"}, {20, "CT"}, 
                {21, "CV"}, {22, "CW"}, {23, "DA"}, {24, "DD"}, {25, "DE"}, 
                {26, "DG"}, {27, "DH"}, {28, "DL"}, {29, "DN"}, {30, "DT"}, 
                {31, "DU"}, {32, "E"}, {33, "EC"}, {34, "EH"}, {35, "EN"}, 
                {36, "EX"}, {37, "FK"}, {38, "FY"}, {39, "G"}, {40, "GL"}, 
                {41, "GU"}, {42, "HA"}, {43, "HD"}, {44, "HG"}, {45, "HP"}, 
                {46, "HR"}, {47, "HS"}, {48, "HU"}, {49, "HX"}, {50, "IG"}, 
                {51, "IP"}, {52, "IV"}, {53, "KA"}, {54, "KT"}, {55, "KW"}, 
                {56, "KY"}, {57, "L"}, {58, "LA"}, {59, "LD"}, {60, "LE"}, 
                {61, "LL"}, {62, "LS"}, {63, "LU"}, {64, "M"}, {65, "ME"}, 
                {66, "MK"}, {67, "ML"}, {68, "N"}, {69, "NE"}, {70, "NG"}, 
                {71, "MM"}, {72, "NP"}, {73, "NR"}, {74, "NW"}, {75, "OL"}, 
                {76, "OX"}]).
-define(DATETIME_FORMAT, "~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w").
-define(DATE_FORMAT, "~b~2..0b~2..0b").

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Ensure that ibrowse is started...
    application:start(ibrowse),

    %% Ensure that riakc library is in the path...
    ensure_module(riakc_pb_socket),
    ensure_module(mochijson2),

    %% Read config settings...
    PBIPs  = basho_bench_config:get(pb_ips, ["127.0.0.1"]),
    PBPort  = basho_bench_config:get(pb_port, 8087),
    HTTPIPs = basho_bench_config:get(http_ips, ["127.0.0.1"]),
    HTTPPort =  basho_bench_config:get(http_port, 8098),
    ReplPBIPs = basho_bench_config:get(replpb_ips, ["127.0.0.1"]),

    PBTimeout = basho_bench_config:get(pb_timeout_general, 30*1000),
    HTTPTimeout = basho_bench_config:get(http_timeout_general, 30*1000),
    FoldTimeout = basho_bench_config:get(fold_timeout_general, 60*60*1000),

    RecordBucket = 
        list_to_binary(
            basho_bench_config:get(record_bucket, "domainRecord")),
    DocumentBucket =
        list_to_binary(
            basho_bench_config:get(document_bucket, "domainDocument")),
    PostCodeIndexCount =
            basho_bench_config:get(postcode_indexcount, 3),
    RecordSyncOnWrite =
        list_to_binary(
            basho_bench_config:get(record_sync, "one")),
    DocumentSyncOnWrite =
        list_to_binary(
            basho_bench_config:get(document_sync, "backend")),
    NodeConfirms = basho_bench_config:get(node_confirms, 2),
    
    %% Choose the target node using our ID as a modulus
    HTTPTargets = basho_bench_config:normalize_ips(HTTPIPs, HTTPPort),
    {HTTPTargetIp, HTTPTargetPort} =
        lists:nth((Id rem length(HTTPTargets) + 1), HTTPTargets),
    ?INFO("Using http target ~p:~p for worker ~p\n",
            [HTTPTargetIp, HTTPTargetPort, Id]),

    %% Choose the target node using our ID as a modulus
    PBTargets = basho_bench_config:normalize_ips(PBIPs, PBPort),
    {PBTargetIp, PBTargetPort} =
        lists:nth((Id rem length(PBTargets) + 1), PBTargets),
    ?INFO("Using pb target ~p:~p for worker ~p\n",
            [PBTargetIp, PBTargetPort, Id]),
    
    ReplTargets = basho_bench_config:normalize_ips(ReplPBIPs, PBPort),
    {ReplTargetIp, ReplTargetPort} =
        lists:nth((Id rem length(ReplTargets) + 1), ReplTargets),
    ?INFO("Using repl target ~p:~p for worker ~p\n", 
            [ReplTargetIp, ReplTargetPort, Id]),

    {AGMaxKC, AGMinKC, AGKeyOrder} = 
        basho_bench_config:get(alwaysget, {1, 1, key_order}),
    {DocSize, DocKeyOrder} =
        basho_bench_config:get(unique, {8000, key_order}),
    
    NodeID = basho_bench_config:get(node_name, node()),
    Host = inet_parse:ntoa(HTTPTargetIp),
    URLFun =
        fun(Bucket) ->
            lists:flatten(
                io_lib:format("http://~s:~p/buckets/~s/props",
                    [Host, HTTPTargetPort, Bucket]))
        end,

    case Id of
        1 ->
            ?INFO("Node ID 1 to set bucket properties", []),
            ?INFO(
                "Setting bucket properties for Record using ~s", 
                [URLFun(RecordBucket)]),
            NodeConfirmsJ =
                mochijson2:encode(
                    {struct,
                        [{<<"props">>,
                            {struct,
                                lists:flatten(
                                    [{<<"node_confirms">>, NodeConfirms}])
                                }}]}),
            SyncOnWriteFun =
                fun(SyncSetting) ->
                    mochijson2:encode(
                        {struct,
                            [{<<"props">>,
                                {struct,
                                    lists:flatten(
                                        [{<<"sync_on_write">>, SyncSetting}])
                                    }}]})
                end,
            ?INFO("Setting node_confirms using ~p", [NodeConfirms]),
            ibrowse:send_req(
                URLFun(RecordBucket),
                [{"Content-Type", "application/json"}],
                put,
                NodeConfirmsJ),
            ibrowse:send_req(
                URLFun(RecordBucket),
                [{"Content-Type", "application/json"}],
                put,
                SyncOnWriteFun(RecordSyncOnWrite)),
            ?INFO("Setting bucket properties for Document Bucket", []),
            ibrowse:send_req(
                URLFun(DocumentBucket),
                [{"Content-Type", "application/json"}],
                put,
                NodeConfirmsJ),
            ibrowse:send_req(
                URLFun(DocumentBucket),
                [{"Content-Type", "application/json"}],
                put,
                SyncOnWriteFun(DocumentSyncOnWrite));
        _ ->
            ok
    end,

    KeyIDint = erlang:phash2(Id) bxor erlang:phash2(NodeID),
    ?INFO("Using Node ID ~w to generate ID ~w\n", [node(), KeyIDint]), 

    case riakc_pb_socket:start_link(PBTargetIp, PBTargetPort) of
        {ok, Pid} ->
            NominatedID = Id == 7,
            ReplPid = 
                case riakc_pb_socket:start_link(ReplTargetIp, ReplTargetPort) of
                    {ok, RP} ->
                        RP;
                    _ ->
                        _ = lager:info("Starting with no repl check"),
                        no_repl_check
                end,
            {ok, #state {
               pb_pid = Pid,
               repl_pid = ReplPid,
               http_host = HTTPTargetIp,
               http_port = HTTPTargetPort,
               recordBucket = RecordBucket,
               documentBucket = DocumentBucket,
               pb_timeout = PBTimeout,
               http_timeout = HTTPTimeout,
               fold_timeout = FoldTimeout,
               query_logfreq = ?QUERYLOG_FREQ,
               nominated_id = NominatedID,
               unique_key_count = 1,
               alwaysget_key_count = 0,
               alwaysget_perworker_maxkeycount = AGMaxKC,
               alwaysget_perworker_minkeycount = AGMinKC,
               alwaysget_keyorder = AGKeyOrder,
               unique_size = DocSize,
               unique_keyorder = DocKeyOrder,
               keyid = <<KeyIDint:32/integer>>,
               postcode_indexcount = PostCodeIndexCount
            }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p port ~p: ~p\n",
                      [PBTargetIp, PBTargetPort, Reason2])
    end.

%% Get a single object.
run(get_pb, KeyGen, _ValueGen, State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.recordBucket,
    Key = to_binary(KeyGen()),
    case riakc_pb_socket:get(Pid, Bucket, Key, State#state.pb_timeout) of
        {ok, _Obj} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

run(alwaysget_http, _KeyGen, _ValueGen, State) ->
    Host = inet_parse:ntoa(State#state.http_host),
    Port = State#state.http_port,
    Bucket = State#state.recordBucket,
    AGKC = State#state.alwaysget_key_count,
    case AGKC > State#state.alwaysget_perworker_minkeycount of 
        true ->
            KeyInt = eightytwenty_keycount(AGKC),    
            Key = generate_uniquekey(KeyInt, State#state.keyid, 
                                        State#state.alwaysget_keyorder),
            URL = 
                io_lib:format("http://~s:~p/buckets/~s/keys/~s", 
                                [Host, Port, Bucket, Key]),

            case get_existing(URL, State#state.http_timeout) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    % not_found is not OK
                    {error, Reason, State}
            end;
        false ->
            {silent, State}
    end;

run(alwaysget_pb, _KeyGen, _ValueGen, State) ->
    % Get one of the objects with unique keys
    Pid = State#state.pb_pid,
    Bucket = State#state.recordBucket,
    AGKC = State#state.alwaysget_key_count,
    case AGKC > State#state.alwaysget_perworker_minkeycount of 
        true ->
            KeyInt = eightytwenty_keycount(AGKC),    
            Key = generate_uniquekey(KeyInt, State#state.keyid, 
                                        State#state.alwaysget_keyorder),

            case riakc_pb_socket:get(Pid, 
                                        Bucket, Key, 
                                        State#state.pb_timeout) of
                {ok, _Obj} ->
                    {ok, State};
                {error, Reason} ->
                    % not_found is not OK
                    {error, Reason, State}
            end;

        false ->
            {silent, State}
        
    end;

run(alwaysget_updatewith2i, _KeyGen, ValueGen, State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.recordBucket,
    AGKC = State#state.alwaysget_key_count,
    Value = ValueGen(),
    KeyInt = eightytwenty_keycount(AGKC),
    ToExtend = 
        rand:uniform(State#state.alwaysget_perworker_maxkeycount) > AGKC,

    {Robj0, NewAGKC} = 
        case ToExtend of 
            true ->
                % Expand the key count
                ExpansionKey = 
                    generate_uniquekey(AGKC + 1, State#state.keyid,
                                        State#state.alwaysget_keyorder),
                case {AGKC rem 1000, State#state.nominated_id} of
                    {0, true} ->
                        _ = lager:info("Always grow key count passing ~w "
                                    ++ "for nominated worker", 
                                [AGKC]);
                    _ ->
                        ok
                end,
                {riakc_obj:new(Bucket, ExpansionKey),
                    AGKC + 1};
            false ->
                % update an existing key
                ExistingKey = 
                    generate_uniquekey(KeyInt, State#state.keyid,
                                        State#state.alwaysget_keyorder),
                {ok, Robj} =
                    riakc_pb_socket:get(Pid, 
                                        Bucket, ExistingKey, 
                                        State#state.pb_timeout),
                {Robj, AGKC}
        end,
    
    MD0 = riakc_obj:get_update_metadata(Robj0),
    MD1 = riakc_obj:clear_secondary_indexes(MD0),
    MD2 =
        riakc_obj:set_secondary_index(
            MD1,
            generate_binary_indexes(State#state.postcode_indexcount)),
    Robj1 = riakc_obj:update_value(Robj0, Value),
    Robj2 = riakc_obj:update_metadata(Robj1, MD2),

    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj2, State#state.pb_timeout) of
        ok ->
            {ok, State#state{alwaysget_key_count = NewAGKC}};
        {error, Reason} ->
            {error, Reason, State}
    end;

run(alwaysget_updatewithout2i, _KeyGen, ValueGen, State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.recordBucket,
    AGKC = State#state.alwaysget_key_count,
    Value = ValueGen(),
    KeyInt = eightytwenty_keycount(AGKC),
    ToExtend = 
        rand:uniform(State#state.alwaysget_perworker_maxkeycount) > AGKC,

    {Robj0, NewAGKC} = 
        case ToExtend of 
            true ->
                % Expand the key count
                ExpansionKey = 
                    generate_uniquekey(AGKC + 1, State#state.keyid,
                                        State#state.alwaysget_keyorder),
                case {AGKC rem 1000, State#state.nominated_id} of
                    {0, true} ->
                        _ = lager:info("Always grow key count passing ~w "
                                    ++ "for nominated worker", 
                                [AGKC]);
                    _ ->
                        ok
                end,
                {riakc_obj:new(Bucket, ExpansionKey),
                    AGKC + 1};
            false ->
                % update an existing key
                ExistingKey = 
                    generate_uniquekey(KeyInt, State#state.keyid,
                                        State#state.alwaysget_keyorder),
                {ok, Robj} =
                    riakc_pb_socket:get(Pid, 
                                        Bucket, ExistingKey, 
                                        State#state.pb_timeout),
                {Robj, AGKC}
        end,
    
    Robj2 = riakc_obj:update_value(Robj0, Value),

    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj2, State#state.pb_timeout) of
        ok ->
            {ok, State#state{alwaysget_key_count = NewAGKC}};
        {error, Reason} ->
            {error, Reason, State}
    end;


%% Update an object with secondary indexes.
run(update_with2i, KeyGen, ValueGen, State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.recordBucket,
    Key = to_binary(KeyGen()),
    Value = ValueGen(),
    
    Robj0 =
        case riakc_pb_socket:get(Pid, Bucket, Key, State#state.pb_timeout) of
            {ok, Robj} ->
                Robj;
            {error, notfound} ->
                riakc_obj:new(Bucket, to_binary(Key))
        end,
    
    MD0 = riakc_obj:get_update_metadata(Robj0),
    MD1 = riakc_obj:clear_secondary_indexes(MD0),
    MD2 =
        riakc_obj:set_secondary_index(
            MD1,
            generate_binary_indexes(State#state.postcode_indexcount)),
    Robj1 = riakc_obj:update_value(Robj0, Value),
    Robj2 = riakc_obj:update_metadata(Robj1, MD2),

    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj2, State#state.pb_timeout) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% Put an object with a unique key and a non-compressable value
run(put_unique_bet365, _KeyGen, _ValueGen, State) ->
    Pid = State#state.pb_pid,
    
    Bucket = 
        case erlang:phash2(Pid) rem 2 of
            0 ->
               <<"abcdefghijklmnopqrstuvwxyz_1">>;
            1 ->
               <<"abcdefghijklmnopqrstuvwxyz_2">>
        end,
    
    UKC = State#state.unique_key_count,
    Key = 
        generate_uniquekey(UKC, 
                            State#state.keyid, 
                            State#state.unique_keyorder),
    
    Value = non_compressible_value(State#state.unique_size),
    
    Robj0 = riakc_obj:new(Bucket, to_binary(Key)),
    MD2 = riakc_obj:get_update_metadata(Robj0),
    Robj1 = riakc_obj:update_value(Robj0, Value),
    Robj2 = riakc_obj:update_metadata(Robj1, MD2),

    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj2, State#state.pb_timeout) of
        ok ->
            {ok, State#state{unique_key_count = UKC + 1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
%% Put an object with a unique key and a non-compressable value
run(put_unique, _KeyGen, _ValueGen, State) ->
    {Pid, _Bucket, _Key, Robj, UKC} = prepare_unique_put(State),
    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj, State#state.pb_timeout) of
        ok ->
            {ok, State#state{unique_key_count = UKC + 1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put_unique_checkrepl, _KeyGen, _ValueGen, State) ->
    {Pid, Bucket, Key, Robj, UKC} = prepare_unique_put(State),
    %% Write the object...
    case riakc_pb_socket:put(Pid, Robj, State#state.pb_timeout) of
        ok ->
            check_repl(State#state.repl_pid, Bucket, to_binary(Key), State#state.pb_timeout),
            {ok, State#state{unique_key_count = UKC + 1}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(get_unique, _KeyGen, _ValueGen, State) ->    
    % Get one of the objects with unique keys
    Pid = State#state.pb_pid,
    Bucket = State#state.documentBucket,
    UKC = State#state.unique_key_count,
    LKC = State#state.unique_key_lowcount,
    Key = generate_uniquekey(LKC + rand:uniform(max(1, UKC - LKC)),
                                State#state.keyid,
                                State#state.unique_keyorder),
    case riakc_pb_socket:get(Pid, Bucket, Key, State#state.pb_timeout) of
        {ok, _Obj} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(delete_unique, _KeyGen, _ValueGen, State) ->
    %% Delete one of the unique keys, assuming that the deletions have not
    %% caught up with the PUTs
    Pid = State#state.pb_pid,
    B = State#state.documentBucket,
    UKC = State#state.unique_key_count,
    LKC = State#state.unique_key_lowcount,
    case LKC < UKC of
        true ->
            Key = generate_uniquekey(LKC,
                                        State#state.keyid,
                                        State#state.unique_keyorder),
            R = riakc_pb_socket:delete(Pid, B, Key, State#state.pb_timeout),
            case R of
                ok ->
                    {ok, State#state{unique_key_lowcount = LKC + 1}};
                {error, Reason} ->
                    {error, Reason, State#state{unique_key_lowcount = LKC + 1}}
            end;
        false ->
            {ok, State}
    end;

%% Query results via the HTTP interface.
run(postcodequery_http, _KeyGen, _ValueGen, State) ->
    Host = inet_parse:ntoa(State#state.http_host),
    Port = State#state.http_port,
    Bucket = State#state.recordBucket,

    L = length(?POSTCODE_AREAS),
    {_, Area} = lists:keyfind(rand:uniform(L), 1, ?POSTCODE_AREAS),
    District = Area ++ integer_to_list(rand:uniform(26)),
    StartPoints = ["ba", "ca", "da", "ea", "fa", "ga", "gb", "gc"],
    StartPoint = lists:nth(rand:uniform(length(StartPoints)), StartPoints),
    StartKey = District ++ "|" ++ StartPoint,
    EndKey = District ++ "|" ++ "gd",
    URL = io_lib:format("http://~s:~p/buckets/~s/index/postcode_bin/~s/~s",
                    [Host, Port, Bucket, StartKey, EndKey]),

    case jsonb_pool_get(URL, State#state.http_timeout) of
        {ok, JsonB} ->
            C0 = State#state.postcodeq_count,
            case C0 rem State#state.query_logfreq of 
                0 ->
                    {struct, Proplist} = mochijson2:decode(JsonB),
                    Results = proplists:get_value(<<"keys">>, Proplist),
                    _ = lager:info(
                        "postcode query result size of ~w",
                        [length(Results)]);
                _ ->
                    ok
            end,
            {ok, State#state{postcodeq_count = C0 + 1}};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n",
                        [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end;

%% Query results via the HTTP interface.
run(dobquery_http, _KeyGen, _ValueGen, State) ->
    Host = inet_parse:ntoa(State#state.http_host),
    Port = State#state.http_port,
    Bucket = State#state.recordBucket,
    
    RandYear = integer_to_list(rand:uniform(70) + 1950),
    RandMonth = integer_to_list(rand:uniform(9)),
    DoBStart = RandYear ++ "0" ++ RandMonth ++ "04",
    DoBEnd = RandYear ++ "0" ++ RandMonth ++ "05",
    
    URLSrc = 
        "http://~s:~p/buckets/~s/index/dateofbirth_bin/~s/~s?term_regex=~s",
    RE= "[0-9]{8}...[a-d]",
    URL = io_lib:format(URLSrc, 
                        [Host, Port, Bucket, DoBStart, DoBEnd, RE]),

    case jsonb_pool_get(URL, State#state.http_timeout) of
        {ok, JsonB} ->
            C0 = State#state.dobq_count,
            case C0 rem State#state.query_logfreq of 
                0 ->
                    {struct, Proplist} = mochijson2:decode(JsonB),
                    Results = proplists:get_value(<<"keys">>, Proplist),
                    _ = lager:info(
                        "dob query result size of ~w",
                        [length(Results)]);
                _ ->
                    ok
            end,
            {ok, State#state{dobq_count = C0 + 1}};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n",
                        [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
        end;

run(aae_query, _KeyGen, _ValueGen, State) ->
    IsAlive =
        case State#state.singleton_pid of
            undefined ->
                false;
            LastPid ->
                is_process_alive(LastPid)
        end,
    case {State#state.nominated_id, IsAlive} of
        {true, true} ->
            _ = lager:info("Skipping aae query for overlap"),
            {ok, State};
        {true, false} ->
            Pid = spawn(?MODULE, run_aaequery, [State]),
            {ok, State#state{singleton_pid = Pid}};
        _ ->
            {ok, State}
    end;

run(list_keys, _KeyGen, _ValueGen, State) ->
    IsAlive =
        case State#state.singleton_pid of
            undefined ->
                false;
            LastPid ->
                is_process_alive(LastPid)
        end,
    case {State#state.nominated_id, IsAlive} of
        {true, true} ->
            _ = lager:info("Skipping listkeys for overlap"),
            {ok, State};
        {true, false} ->
            Pid = spawn(?MODULE, run_listkeys, [State]),
            {ok, State#state{singleton_pid = Pid}};
        _ ->
            {ok, State}
    end;

run(segment_fold, _KeyGen, _ValueGen, State) ->
    IsAlive = 
        case State#state.singleton_pid of
            undefined ->
                false;
            LastPid ->
                is_process_alive(LastPid)
        end,
    case {State#state.nominated_id, IsAlive} of
        {true, true} ->
            _ = lager:info("Skipping segment fold for overlap"),
            {ok, State};
        {true, false} ->
            Pid = spawn(?MODULE, run_segmentfold, [State]),
            {ok, State#state{singleton_pid = Pid}};
        _ ->
            {ok, State}
    end;

run(force_aae, KeyGen, ValueGen, State) ->
    SinceLastForceSec = 
        timer:now_diff(os:timestamp(), State#state.last_forceaae)/1000000,
    case {State#state.nominated_id, SinceLastForceSec > ?FORCEAAE_FREQ} of
        {true, true} ->
            Host = inet_parse:ntoa(State#state.http_host),
            Port = State#state.http_port,
            Bucket = binary_to_list(State#state.recordBucket),
            Key = KeyGen(),
            Timeout = State#state.http_timeout,

            URLSrc = "http://~s:~p/buckets/~s/keys/~p?force_aae=true",
            URL = io_lib:format(URLSrc, [Host, Port, Bucket, Key]),
            Target = lists:flatten(URL),

            case ibrowse:send_req(Target, [], get, [], [], Timeout) of
                {ok, "200", _, _Body} ->
                    {ok, State#state{last_forceaae = os:timestamp()}};
                {ok, "404", _, _NotFound} ->
                    {ok, State#state{last_forceaae = os:timestamp()}};
                Other ->
                    {error, Other, State}
            end;
        _ ->
            run(get_pb, KeyGen, ValueGen, State)
    end;

run(Other, _, _, _) ->
    throw({unknown_operation, Other}).

%% ====================================================================
%% Internal functions
%% ====================================================================


prepare_unique_put(State) ->
    Pid = State#state.pb_pid,
    Bucket = State#state.documentBucket,
    
    UKC = State#state.unique_key_count,
    Key = 
        generate_uniquekey(UKC, 
                            State#state.keyid, 
                            State#state.unique_keyorder),
    
    Value = non_compressible_value(State#state.unique_size),
    
    Robj0 = riakc_obj:new(Bucket, to_binary(Key)),
    MD1 = riakc_obj:get_update_metadata(Robj0),
    MD2 = riakc_obj:set_secondary_index(MD1, generate_binary_indexes()),
    Robj1 = riakc_obj:update_value(Robj0, Value),
    Robj2 = riakc_obj:update_metadata(Robj1, MD2),
    {Pid, Bucket, Key, Robj2, UKC}.

jsonb_pool_get(Url, Timeout) ->
    Target = lists:flatten(Url),
    Response = ibrowse:send_req(Target, [], get, [], [], Timeout),
    case Response of
        {ok, "200", _, Body} ->
            {ok, Body};
        Other ->
            {error, Other}
    end.

json_direct_get(Url, Timeout) ->
    Target = lists:flatten(Url),
    {ok, C} = ibrowse:spawn_worker_process(Target),
    Response = ibrowse:send_req_direct(C, Target, [], get, [], [], Timeout),
    case Response of
        {ok, "200", _, Body} ->
            {ok, mochijson2:decode(Body)};
        Other ->
            {error, Other}
    end.

get_existing(Url, Timeout) ->
    case ibrowse:send_req(lists:flatten(Url), [], get, [], [], Timeout) of
        {ok, "200", _, _Body} ->
            ok;
        Other ->
            {error, Other}
    end.


to_binary(B) when is_binary(B) ->
    B;
to_binary(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I));
to_binary(L) when is_list(L) ->
    list_to_binary(L).

ensure_module(Module) ->
    case code:which(Module) of
        non_existing ->
            ?FAIL_MSG("~s requires " ++ atom_to_list(Module) ++ 
                            " module to be available on code path.\n", 
                        [?MODULE]);
        _ ->
            ok
    end.

check_repl(no_repl_check, _B, _K, _TO) ->
    ok;
check_repl(ReplPid, Bucket, Key, Timeout) ->
    case riakc_pb_socket:get(ReplPid, Bucket, Key, Timeout) of
        {ok, _Obj} ->
            ok;
        {error, _} ->
            check_repl(ReplPid, Bucket, Key, Timeout)
    end.

%% ====================================================================
%% Spawned Runners
%% ====================================================================


run_aaequery(State) ->
    SW = os:timestamp(),
    _ = lager:info("Commencing aaequery request"),

    Host = inet_parse:ntoa(State#state.http_host),
    Port = State#state.http_port,
    Bucket = State#state.recordBucket,

    KeyStart = "0", 
    KeyEnd = "z",

    MapFoldMod = "riak_kv_tictac_folder",

    URLSrc = 
        "http://~s:~p/buckets/~s/index/$key/~s/~s?mapfold=true&mapfoldmod=~s",
    URL = io_lib:format(URLSrc, 
                        [Host, Port, Bucket, KeyStart, KeyEnd, MapFoldMod]),
    
    case json_direct_get(URL, State#state.fold_timeout) of
        {ok, {struct, TreeL}} ->
            {<<"count">>, Count} = lists:keyfind(<<"count">>, 1, TreeL),
            _ = lager:info("AAE query returned in ~w seconds covering ~s keys",
                      [timer:now_diff(os:timestamp(), SW)/1000000, Count]),

            {ok, State};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n",
                        [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end.

run_listkeys(State) ->
    SW = os:timestamp(),
    _ = lager:info("Commencing list keys request"),

    Host = inet_parse:ntoa(State#state.http_host),
    Port = State#state.http_port,
    Bucket = State#state.recordBucket,

    URLSrc = 
        "http://~s:~p/buckets/~s/keys?keys=true",
    URL = io_lib:format(URLSrc, 
                        [Host, Port, Bucket]),
    
    case json_direct_get(URL, State#state.fold_timeout) of
        {ok, {struct, [{<<"keys">>, KeyList}]}} ->
            _ = lager:info("List keys returned ~w keys in ~w seconds",
                      [length(KeyList), 
                        timer:now_diff(os:timestamp(), SW)/1000000]),

            {ok, State};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n",
                        [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end.


run_segmentfold(State) ->
    SW = os:timestamp(),
    _ = lager:info("Commencing segment fold request"),

    Host = inet_parse:ntoa(State#state.http_host),
    Port = State#state.http_port,
    Bucket = State#state.recordBucket,

    KeyStart = "0", 
    KeyEnd = "z",

    MapFoldMod = "riak_kv_segment_folder",

    % '{"check_presence": "false", 
    %       "tree_size": "small", 
    %       "segment_list": [1, 10001]}'
    MapFoldOpts = 
        "eyJjaGVja19wcmVzZW5jZSI6ICJmYWxzZSIsICJ0cmVlX3NpemUiOiAic21hbGwiLCA"
            ++ "ic2VnbWVudF9saXN0IjogWzEsIDEwMDAxXX0=",

    URLSrc = 
        "http://~s:~p/buckets/~s/index/$key/~s/~s?mapfold=true&mapfoldmod=~s"
            ++ "&mapfoldoptions=~s",
    URL = io_lib:format(URLSrc, 
                        [Host, Port, Bucket, KeyStart, KeyEnd, 
                            MapFoldMod, MapFoldOpts]),
    
    case json_direct_get(URL, State#state.fold_timeout) of
        {ok, {struct, [{<<"deltas">>, SegL}]}} ->
            _ = lager:info("Segment fold returned in ~w seconds finding ~w keys",
                      [timer:now_diff(os:timestamp(), SW)/1000000, length(SegL)]),
            {ok, State};
        {error, Reason} ->
            io:format("[~s:~p] ERROR - Reason: ~p~n",
                        [?MODULE, ?LINE, Reason]),
            {error, Reason, State}
    end.

%% ====================================================================
%% Index seeds
%% ====================================================================

generate_binary_indexes() ->
    [{{binary_index, "lastmodified"}, lastmodified_index()}].

generate_binary_indexes(PCIdxCount) ->
    [{{binary_index, "postcode"}, postcode_index(PCIdxCount)},
        {{binary_index, "dateofbirth"}, dateofbirth_index()},
        {{binary_index, "lastmodified"}, lastmodified_index()}].

postcode_index(PCIdxCount) ->
    NotVeryNameLikeThing = base64:encode_to_string(crypto:strong_rand_bytes(4)),
    lists:map(fun(_X) -> 
                    L = length(?POSTCODE_AREAS),
                    {_, Area} = lists:keyfind(rand:uniform(L), 1, ?POSTCODE_AREAS),
                    District = Area ++ integer_to_list(rand:uniform(26)),
                    F = District ++ "|" ++ NotVeryNameLikeThing,
                    list_to_binary(F) end,
                lists:seq(1, rand:uniform(PCIdxCount))).

dateofbirth_index() ->
    Delta = rand:uniform(2500000000),
    {{Y, M, D},
        _} = calendar:gregorian_seconds_to_datetime(Delta + 61000000000),
    F = 
        lists:flatten(
            io_lib:format(?DATE_FORMAT, 
                [Y, M, D])) ++ "|" ++ 
                base64:encode_to_string(crypto:strong_rand_bytes(4)),
            [list_to_binary(F)].

lastmodified_index() ->
    {{Year, Month, Day},
        {Hr, Min, Sec}} = calendar:now_to_datetime(os:timestamp()),
    F = lists:flatten(io_lib:format(?DATETIME_FORMAT,
                                        [Year, Month, Day, Hr, Min, Sec])),
    [list_to_binary(F)].
    

generate_uniquekey(C, RandBytes, skew_order) ->
    H0 = convert_tolist(erlang:phash2(C)),
    RB = convert_tolist(RandBytes),
    <<H0/binary, RB/binary>>;
generate_uniquekey(C, RandBytes, key_order) ->
    B0 = convert_tolist(C),
    RB = convert_tolist(RandBytes),
    <<B0/binary, RB/binary>>.


non_compressible_value(Size) ->
    crypto:strong_rand_bytes(Size).


eightytwenty_keycount(UKC) ->
    % 80% of the time choose a key in the bottom 20% of the 
    % result range, and 20% of the time in the upper 80% of the range
    TwentyPoint = rand:uniform(max(1, UKC div 5)),
    case rand:uniform(max(1, UKC)) < TwentyPoint of
        true ->
            rand:uniform(UKC - TwentyPoint) + max(1, TwentyPoint);
        false ->
            rand:uniform(max(1, TwentyPoint))
    end.


convert_tolist(I) when is_integer(I) ->
    list_to_binary(lists:flatten(io_lib:format("~9..0B", [I])));
convert_tolist(Bin) ->
    <<I:26/integer, _Tail:6/bitstring>> = Bin,
    convert_tolist(I).

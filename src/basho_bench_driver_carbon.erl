-module(basho_bench_driver_carbon).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {
        host,
        port,
        keep_connect,
        batch_size,
        socket
    }).

new(_Id) ->
    Host = basho_bench_config:get(carbon_server, "127.0.0.1"),
    Port = basho_bench_config:get(carbon_port, 2003),
    KeepConnect = basho_bench_config:get(carbon_keep_connect, false),
    BatchSize = basho_bench_config:get(carbon_batch_size, 100),
    {ok, #state{
            host=Host,
            port=Port,
            keep_connect=KeepConnect,
            batch_size=BatchSize,
            socket=nil}}.

carbon(State = #state{host=Host, port=Port, socket=nil}, Message) ->
    case gen_tcp:connect(Host, Port, [list, {packet, 0}]) of
        {ok, Sock} ->
            carbon(State#state{socket=Sock}, Message);
        Error ->
            Error
    end;

carbon(State = #state{socket=Socket, keep_connect=KeepConnect}, Message) ->
    case gen_tcp:send(Socket, Message) of
        ok ->
            case KeepConnect of
                true ->
                    {ok, State};
                _ ->
                    ok = gen_tcp:close(Socket),
                    {ok, State#state{socket=nil}}
            end;
        Error ->
            Error % let it crash
    end.

concat(0, _Keygen, _Ts, List) ->
    List;

concat(Size, KeyGen, Ts, List) ->
    Msg = io_lib:format("pim.pam.poum.~p ~p ~p~n", [KeyGen(), Ts,  random:uniform(1000)]),
    concat(Size -1, KeyGen, Ts, [Msg | List]).

run(set, KeyGen, _ValueGen, State = #state{batch_size=BatchSize}) ->
    {Mega, Sec, _Micro} = now(),
    Msg = concat(BatchSize, KeyGen, Mega * 1000 + Sec, []),

    case carbon(State, Msg) of
        {error, E} ->
            {error, E, State};
        OK ->
            OK
    end.


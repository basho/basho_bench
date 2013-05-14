-module(basho_bench_driver_carbon).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {
        host,
        port,
        keys
    }).

new(_Id) ->
    Host = basho_bench_config:get(carbon_server, "127.0.0.1"),
    Port = basho_bench_config:get(carbon_port, 2003),
    Keys = basho_bench_config:get(carbon_keys, 10),
    {ok, #state{host=Host, port=Port, keys=Keys}}.

run(set, KeyGen, _ValueGen, State) ->
     % io:format("SENDING: ~s\n", [Msg]),
    case gen_tcp:connect(State#state.host,
                         State#state.port,
                         [list, {packet, 0}]) of
        {ok, Sock} ->
            {Mega, Sec, _Micro} = now(),
            Msg = io_lib:format("pim.pam.poum.~p ~p ~p~n", [KeyGen(), (Mega * 1000 + Sec),  random:uniform(1000)]),
            gen_tcp:send(Sock, Msg),
            gen_tcp:close(Sock),
            ok;
        E ->
            %error_logger:error_msg("Failed to connect to graphite: ~p", [E]),
            E
    end,
    {ok, State}.


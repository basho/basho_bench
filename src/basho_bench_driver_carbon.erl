-module(basho_bench_driver_carbon).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {
        host,
        port
    }).

new(_Id) ->
    Host = basho_bench_config:get(carbon_server, "127.0.0.1"),
    Port = basho_bench_config:get(carbon_port, 2003),
    {ok, #state{host=Host, port=Port}}.

run(set, KeyGen, _ValueGen, State) ->
    case gen_tcp:connect(State#state.host,
                         State#state.port,
                         [list, {packet, 0}]) of
        {ok, Sock} ->
            {Mega, Sec, _Micro} = now(),
            Msg = io_lib:format("pim.pam.poum.~p ~p ~p~n", [KeyGen(), (Mega * 1000 + Sec),  random:uniform(1000)]),
            gen_tcp:send(Sock, Msg),
            gen_tcp:close(Sock),
            {ok, State};
        Error ->
            {error, Error, State}
    end.


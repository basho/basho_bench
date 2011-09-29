-module(basho_bench_searchgen).

-export([new/2]).
-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new({function, Module, Function, Args}, Id) ->
    io:format("create searchgen~n"),
    case code:ensure_loaded(Module) of
        {module, Module} ->
            erlang:apply(Module, Function, [Id] ++ Args);
        _Error ->
            ?FAIL_MSG("Could not find searchgen function: ~p:~p\n", [Module, Function])
    end.

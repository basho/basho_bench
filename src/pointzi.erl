-module(pointzi).
-export([
         generate_install/1
]).
generate_install(_Id) ->
    fun() ->
        mochijson2:encode({struct, [
            {<<"id">>, list_to_binary(basho_uuid:to_string(basho_uuid:v4()))},
            {<<"app_key">>, <<"LOTSAINSTALLS2">>},
            {<<"client_version">>, <<"1.2.3">>},
            {<<"ipaddress">>, <<"192.168.0.1">>},
            {<<"model">>, <<"basho_bench">>},
            {<<"operating_system">>, <<"web">>},
            {<<"sh_version">>, <<"1.9.23">>},
            {<<"pz_version">>, <<"2.0">>},
            {<<"utc_offset">>, 166}
        ]})
     end.
                      

generate_tag(_Id) ->
    fun() ->
        mochijson2:encode({struct, [
            {<<"id">>, list_to_binary(basho_uuid:to_string(basho_uuid:v4()))},
            {<<"installid">>, list_to_binary(basho_uuid:to_string(basho_uuid:v4()))},
            {<<"app_key">>, <<"LOTSAINSTALLS2">>},
            {<<"client_version">>, <<"1.2.3">>},
            {<<"ipaddress">>, <<"192.168.0.1">>},
            {<<"model">>, <<"basho_bench">>},
            {<<"operating_system">>, <<"web">>},
            {<<"sh_version">>, <<"1.9.23">>},
            {<<"pz_version">>, <<"2.0">>},
            {<<"utc_offset">>, 166},
        ]})
     end.

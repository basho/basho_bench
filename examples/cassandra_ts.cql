 % CREATE TABLE demo.GeoCheckin (myfamily varchar, myseries varchar, time timestamp, time_quanta timestamp, myint bigint, mytext varchar, myfloat double, mybool boolean, PRIMARY KEY ((myfamily, myseries), time_quanta));

% CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};


{mode, max}.

{duration, 1}.

{concurrent, 1}.

{report_interval, 1}.

{driver, basho_bench_driver_cassandra_cql}.

{key_generator, {partitioned_sequential_int, 50000, 1000000000}}.

{value_generator, {fixed_bin, 100}}.

{cassandra_ips, [{"localhost", 9042}]}.
%%{cassandra_port, 9042}.

{cassandra_keyspace, "DEMO"}.
{cassandra_columnfamily, "GeoCheckin"}.

{cassandra_read_consistency, 1}. % CQERL_CONSISTENCY_ONE
{cassandra_write_consistency, 1}. % CQERL_CONSISTENCY_ONE


{operations, [{ts_put, 1}]}.

{code_paths, ["./deps/cqerl/ebin", "./deps/pooler/ebin",  "./deps/uuid/ebin",
              "./deps/semver/ebin"]}.

{application, basho_bench,
 [{description, "Riak Benchmarking Suite"},
  {vsn, "0.1"},
  {modules, [
             basho_bench,
             basho_bench_app,
             basho_bench_config,
             basho_bench_driver_2i,
             basho_bench_driver_dets,
             basho_bench_driver_http_raw,
             basho_bench_driver_innostore,
             basho_bench_driver_riakc_pb,
             basho_bench_driver_riakclient,
             basho_bench_driver_cassandra,
             basho_bench_driver_bitcask,
             basho_bench_driver_hibari,
             basho_bench_driver_null,
             basho_bench_driver_riakc_java,
             basho_bench_java_client,
             basho_bench_log,
             basho_bench_measurement,
             basho_bench_measurement_erlangvm,
             basho_bench_keygen,
             basho_bench_stats,
             basho_bench_sup,
             basho_bench_worker,
             basho_bench_valgen
             ]},
  {registered, [ basho_bench_sup ]},
  {applications, [kernel,
                  stdlib,
                  sasl]},
  {mod, {basho_bench_app, []}},
  {env, [
         %%
         %% Mode of load generation:
         %% max - Generate as many requests as possible per worker
         %% {rate, Rate} - Exp. distributed Mean reqs/sec
         %%
         {mode, {rate, 5}},

         %%
         %% Default log level
         %%
         {log_level, debug},

         %%
         %% Base test output directory
         %%
         {test_dir, "tests"},

         %%
         %% Test duration (minutes)
         %%
         {duration, 5},

         %%
         %% Number of concurrent workers
         %%
         {concurrent, 3},

         %%
         %% Driver module for the current test
         %%
         {driver, basho_bench_driver_http_raw},

         %%
         %% Operations (and associated mix). Note that
         %% the driver may not implement every operation.
         %%
         {operations, [{get, 4},
                       {put, 4},
                       {delete, 1}]},

         %%
         %% Interval on which to report latencies and status (seconds)
         %%
         {report_interval, 10},

         %%
         %% Key generators
         %%
         %% {uniform_int, N} - Choose a uniformly distributed integer between 0 and N
         %%
         {key_generator, {uniform_int, 100000}},

         %%
         %% Value generators
         %%
         %% {fixed_bin, N} - Fixed size binary blob of N bytes
         %%
         {value_generator, {fixed_bin, 100}},

         %%
         %% RNG Seed -- ensures consistent generation of key/value sizes (but not content!)
         %%
         {rng_seed, {42, 23, 12}}
        ]}
]}.

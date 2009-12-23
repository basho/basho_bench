{application, riak_bench,
 [{description, "Riak Benchmarking Suite"},
  {vsn, "0.1"},
  {modules, [
             riak_bench,
             riak_bench_app,
             riak_bench_config,
             riak_bench_driver_http_raw,
             riak_bench_driver_innodb,
             riak_bench_log,
             riak_bench_keygen,              
             riak_bench_stats,
             riak_bench_sup,
             riak_bench_worker,
             riak_bench_valgen
             ]},
  {registered, [ riak_bench_sup ]},
  {applications, [kernel, 
                  stdlib, 
                  sasl]},
  {mod, {riak_bench_app, []}},
  {env, [
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
         {driver, riak_bench_driver_http_raw},

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

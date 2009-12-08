{application, riak_bench,
 [{description, "Riak Benchmarking Suite"},
  {vsn, "1"},
  {modules, [ riak_bench_app,
              riak_bench_sup ]},
  {registered, [ riak_bench_sup ]},
  {applications, [kernel, 
                  stdlib, 
                  sasl]},
  {mod, {riak_bench_app, []}},
  {env, [
         %%
         %% Load generation modes:
         %%
         %% peak - Generates requests as quickly as possible.
         %%        ex: peak
         %%
         %% sustained - Generates a sustained load w/ a specified total # of requests/sec OR
         %%             random inter-arrival time (in milliseconds) between requests
         %%             ex: {sustained, {rate, 100}} == sustained load of 100 req/sec
         %%                 {sustained, {delay, 10}} == sustained load w/ 10 ms between requests
         %%
         {load_gen_mode, peak},

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
         {driver, simple},

         %% Operations (and associated mix). Note that
         %% the driver may not implement every operation.
         {operations, [{get, 45},
                       {put, 45},
                       {delete, 10}]},

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
         {value_generator, {fixed_bin, 100}}
        ]}
]}.

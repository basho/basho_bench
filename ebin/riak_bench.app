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
         %% Load generation mode: peak or sustained
         {load_gen_mode, peak},

         %% Test duration (minutes)
         {duration, 5},

         %% Number of concurrent workers
         {concurrent, 3},

         %% Driver for the current test
         {driver, jiak},

         %% Operations (and associated mix). Note that
         %% the driver may not implement every operation.
         {operations, [{get, 45},
                       {put, 45},
                       {delete, 10}]}

         %% 
         
        ]}
]}.

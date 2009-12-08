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
  {env, []}
]}.

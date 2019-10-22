{suites, "../test", all}.
{skip_suites, "../test", [spg_benchmark_SUITE], "Benchmarks are not a part of normal run"}.
{skip_suites, "../test", [spg_cluster_SUITE], "Property-based testing takes up to 24 hours to complete, run it separately with 'rebar3 ct --suite spg_cluster_SUITE'"}.

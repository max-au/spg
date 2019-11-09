{suites, "../test", all}.
{skip_suites, "../test", [spg_benchmark_SUITE], "Benchmarks are not a part of normal run"}.
{skip_suites, "../test", [spg_cluster_SUITE], "Property-based test may take up to 2 hours, use 'rebar3 ct --suite spg_cluster_SUITE' to run this test"}.

{suites, "../test", all}.
{skip_suites, "../test", [spg_cluster_SUITE], "Property-based test takes more than 1 hour and requires OTP 25. Use 'rebar3 ct --suite spg_cluster_SUITE' to run this test"}.

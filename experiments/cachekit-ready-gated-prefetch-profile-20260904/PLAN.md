# CacheKit ready-gated prefetch profiling plan

## Question

Why does the proven-active `ready-d2` path regress q9 K/s/core on both tested
hosts, despite staging and consuming millions of ValueState values?

## Scope

- Profile the existing P1 control and `ready-d2` artifacts without changing
  runtime code or configuration.
- Use Kunpeng NUMA node 0 CPUs 38--74 and memory node 0. A foreign job is
  allowed only when every foreign container has an explicit CPU set disjoint
  from this campaign.
- Preserve q9, 100,000,000 events, 8 TaskManagers, 16 slots, no periodic
  checkpointing, and the repaired disjoint CPU-metric ownership collector.
- Treat throughput from profiler-attached legs as diagnostic only. The sealed
  P1 paired runs remain the performance evidence.

## Profiles

After each leg's runner enters the real measurement window, select the highest
CPU `TaskManagerRunner` JVM in each TaskManager container and collect:

- 60 seconds CPU at 5 ms sampling interval, thread-separated, collapsed output.
- 45 seconds allocation sampling, thread-separated, collapsed output.

The profiler is copied into the owned campaign containers and every output is
copied to the host experiment directory before Compose cleanup. No profiler is
attached to a foreign container or process.

## Decision use

Use the profiles to choose one bounded implementation change. Re-run focused
correctness tests, then a same-artifact paired canary. Do not claim a win from
profiled throughput or from an inactive query. The optimization objective is
the arithmetic mean of paired per-query K/s/core uplift percentages over the
queries that independently prove positive path activation.

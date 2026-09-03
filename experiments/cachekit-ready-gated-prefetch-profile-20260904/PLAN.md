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

- 60 seconds CPU at 5 ms sampling interval, thread-separated, collapsed output;
  if the container cannot access perf events, record that check and use wall-clock
  sampling at the same interval.
- 45 seconds allocation sampling, thread-separated, collapsed output.

The profiler is copied into the owned campaign containers and every output is
copied to the host experiment directory before Compose cleanup. No profiler is
attached to a foreign container or process.

Attempt 1 used the redirected runner log as its phase signal. Python block
buffering kept that file empty until the leg ended, so no profiler was attached
and the attempt is diagnostic infrastructure failure only. Attempt 2 instead
waits for at least three live `Current Cores=` samples in the owned Compose
project's `nexmark-logs` volume. Target selection reads process-level procfs
records because procps can stall while enumerating the containers' many JVM
threads. Profiler commands enter the owned container's mount and PID namespaces
directly; on this host, Docker exec itself can remain stuck after the
in-container command has exited.

Attempt 2 validated the live phase signal but its first attachment used procps
through Docker exec and did not reach the profiler before control completed.
The benchmark remains diagnostic-only and runs to completion; attempt 3 uses
the procfs plus namespace-entry path above. Capability checks on attempt 2 show
that CPU perf events are denied while wall-clock and allocation events work.

## Decision use

Use the profiles to choose one bounded implementation change. Re-run focused
correctness tests, then a same-artifact paired canary. Do not claim a win from
profiled throughput or from an inactive query. The optimization objective is
the arithmetic mean of paired per-query K/s/core uplift percentages over the
queries that independently prove positive path activation.

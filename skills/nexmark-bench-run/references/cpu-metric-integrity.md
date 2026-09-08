# Nexmark CPU metric integrity

Read this reference when a run reports CPU cores, throughput/core, or a per-core
uplift, or when measured cores exceed the container CPU capacity.

## Failure mechanism

The historical 2x4 entrypoint starts three TaskManagers with
`taskmanager.sh start`, then replaces PID 1 with a fourth TaskManager using
`start-foreground`. The three daemon JVMs retain PID 1 as their OS parent.

`CpuMetricSender` discovers all four `TaskManagerRunner` PIDs and creates one
recursive `ProcfsBasedProcessTree` per PID. The foreground TM tree already
contains the other three TMs, while the other three trees measure them again.
`CpuMetricReceiver.getTotalCpu()` then sums every `host:pid` value without
deduplicating descendants.

For one container:

```text
reported = cpu(all four TMs) + cpu(the three child TMs)
```

With balanced TM load, the amplification is `7/4 = 1.75x`. This is a useful
diagnostic signature, not a generally valid correction factor.

## Required gate

Run `scripts/check_cpu_metric_process_trees.py` after cluster startup and before
warmup. It uses host-side `docker top`; it does not attach to or execute inside
the containers.

Acceptance requires all of the following:

1. The expected number of `TaskManagerRunner` JVMs is present.
2. No monitored TM PID is an ancestor of another monitored TM PID in the same
   container.
3. Runtime `cpuset.cpus.effective` and `cpu.max` match the experiment identity.
4. Sampled total cores do not exceed the effective CPU capacity, apart from a
   small explicitly documented sampling tolerance.
5. A controlled CPU load agrees with cgroup `cpu.stat:usage_usec` deltas.

`BENCH_EXPECTED_TMS` verifies metric coverage only. It cannot detect overlapping
process trees.

## Correct repairs

Choose one and record it in the experiment identity.

### Make TM JVMs siblings

Keep a non-TM init/supervisor as container PID 1 and launch all TM JVMs as its
sibling children. The supervisor must forward termination signals and reap
children. Do not make one monitored TM the parent of the others.

### Make collector ownership disjoint

The collector may retain recursive process accounting, but every process must be
assigned to exactly one monitored TM root. If a monitored TM root appears below
another monitored TM root, exclude that nested subtree from the ancestor's
metric. Preserve one metric identity per Flink TM so coverage remains auditable.

Measuring only the container-wide cgroup CPU is also valid for total cluster CPU,
but it changes the metric identity and must not be presented as per-TM coverage.

## Result handling

- Raw throughput and event counts are not changed by this CPU accounting bug.
- Historical per-core rows from an overlapping topology are invalid unless raw
  per-TM samples allow an exact reconstruction.
- Do not assume paired backend ratios cancel the error: the duplicated TMs have
  weight two while the foreground TM has weight one, so variant-dependent load
  skew can bias the ratio.
- Store the preflight output, effective cgroup limits, and CPU-capacity check with
  the final experiment evidence.

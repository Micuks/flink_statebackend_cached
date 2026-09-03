# CacheKit q9 ready-gate profile analysis

## Result

The `ready-d2` path is genuinely active but spends too much time waiting for a
short worker operation. The first bounded response is a same-artifact depth-4
screen. If that does not turn positive, further depth/timeout tuning is not a
credible route to the greater-than-10% objective.

The profiler-attached attempt is diagnostic only:

| Variant | Raw K/s/core | Raw K/s | Cores | Valid |
| --- | ---: | ---: | ---: | --- |
| control | 25.47 | 392.88 | 15.43 | yes |
| ready-d2 | 25.23 | 388.44 | 15.39 | yes |

Its paired diagnostic uplift is `-0.9422850412249661%`. Both legs completed the
real 100M-event job with 8-TM CPU coverage. This throughput is not used as the
sealed P1 performance result because profiling was attached.

## Activation and latency

- Activation passed for all 32 positive ready-gate series.
- The ready batch rate was `0.254005`; the staged-value consumption rate was
  `0.986961`.
- `4,041,313` values were staged and `3,988,619` were consumed; worker failure
  and staging-admission fallbacks were zero.
- Weighted worker queue time was `720.81 us`, versus only `28.58 us` weighted
  worker service time, a queue/service ratio of about `25.2x`.
- The Prometheus aggregate recorded `506.66 s` of ring-full time and `365.11 s`
  of prefetch wait time. These are sums across series, not job wall time.

## Profile attribution

CPU perf events were denied by the host, so the recorded `cpu.collapsed` files
contain 60-second wall-clock samples. Allocation profiles ran for 45 seconds.
The summary combines the busiest TaskManager JVM from each owned container.

Within the Rank-thread wall-clock scope, `LockSupport.parkNanos` occupied
`72.52%` for `ready-d2`, versus `58.68%` for control. This is the strongest
causal signal: a depth-2 full ring disables the default input action and parks
the mailbox while a short completion task waits behind queued work.

Within Rank-thread allocation samples, candidate-only inclusive paths were
`sealReadyBatch` at `10.33%`, `prefetchWithCompletion` at `9.97%`, and fallback
reservation cancellation at `5.86%`; these inclusive percentages overlap and
must not be added. The prefetch worker accounted for only `0.04%` of all
candidate allocation samples. Within that small worker scope, staging and
MultiGet were visible, but optimizing their allocations first would not address
the mailbox parking boundary.

## Decision sequence

1. Test the already-supported four-batch ring with a fresh paired control.
2. If depth 4 is non-positive, reject depths 2--4 and do not hide the result by
   extending an unvalidated retained-record queue.
3. The next implementation candidate is an opt-in immediate MultiGet path for
   exact next-batch ValueState keys: execute the roughly 29-us state preparation
   on the mailbox, then consume the staged values immediately. This removes the
   roughly 721-us shared-worker queue, ring futures, timeout scheduling, and
   record retention while keeping operator execution and mutation on the
   mailbox. It requires separate correctness tests and activation counters
   before a performance artifact is built.

## Evidence

- Remote experiment:
  `/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-profile-q9-100m-kunpeng-20260904-a3`
- Artifact SHA-256:
  `97487e04d293f154b8856e43decf2573fabaaf35e36077ea635a1f55d0f59dfc`
- Runtime source commit:
  `5a9d1e656715a403afac72ee1a516876ddbfb7f1`
- Local aggregate: `remote-evidence/attempt3/PROFILE_SUMMARY.{json,md}`
- Raw collapsed stacks remain local and remote with per-profile SHA-256 files;
  they are intentionally ignored by Git because the four valid profile pairs
  total tens of megabytes.

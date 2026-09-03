# CacheKit ready-gated depth-4 q9 screen

## Question

Does raising the already-supported ordered ready-gate ring from two to four
batches remove enough mailbox parking to make the path profitable on q9?

## Profile basis

The paired Kunpeng attempt-3 profiles used the unchanged P1 artifact. Within
the Rank task wall-clock scope, `LockSupport.parkNanos` accounted for 72.52% in
`ready-d2` versus 58.68% in control. The candidate also accumulated 516.01 s of
ring-full time across its Prometheus series. Worker service averaged about 27
us, while worker queue time averaged about 722 us. This screen changes only the
gate enablement and the supported ring-depth setting; it does not change code.

## Frozen execution

- Kunpeng NUMA node 0 CPUs 38--74 and memory node 0.
- q9, 100,000,000 events, 8 TaskManagers, 16 slots, no periodic checkpointing.
- Same P1 AArch64 artifact and runtime dependencies for both legs.
- Order: `control`, then `ready-d4`.
- A foreign campaign is allowed only when all of its containers have explicit
  CPU sets disjoint from this campaign.
- The prior RocksDB and P1 control results are secondary references only; the
  causal denominator is the control in this campaign.

## Decision

Require valid legs and independent positive activation. If `ready-d4` remains
non-positive, reject further tuning of depths 2--4 and use the profiles to move
to another bounded implementation path. The user's terminal objective remains
greater than 10% arithmetic-mean K/s/core uplift over effective queries.

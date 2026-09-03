# P1 ready-gated prefetch implementation plan

Source boundary: runtime source `7a9e568dcbd75ab4e24e7a33a4339f5cd1cf8000`.
P1 is opt-in and defaults off. Existing record-at-a-time and batch paths must be
byte-for-byte behaviorally unchanged when it is disabled.

## Invariants

1. A sealed batch progresses `FILLING -> PREFETCHING -> READY -> DISPATCHING`.
2. The ring is bounded. Begin with two in-flight batches; only screen depths
   three and four after correctness and q9 activation pass.
3. Worker threads may only publish immutable completion/results. Record
   emission, key context, state/timer access, and output remain mailbox-thread
   operations.
4. Batches dispatch in sequence. A later completion never overtakes the head.
5. A full ring whose head is not ready suspends default input processing through
   the input processor availability future while mailbox mails remain runnable.
6. Failure, queue rejection/drop, timeout, missing state, selector failure, or
   generation mismatch converts the affected batch to authoritative dispatch;
   no record is lost, duplicated, or reordered.
7. Object reuse fails closed to the legacy path until retained records have an
   explicit ownership/copying contract.
8. Watermarks, stream status, latency markers, end-of-input, checkpoint
   preparation, cancellation, and disposal fence or release every older batch.

## Incremental slices

### Slice 1: completion-bearing backend hook

- Add a separate `prefetchWithCompletion` backend hook; preserve the existing
  void hook.
- Aggregate every eligible ValueState wrapper into one batch completion.
- Complete only after staging publication; propagate build failures, worker
  failures, queue drops/rejections, and cancellation.
- Unit-test success, multiple wrappers, no eligible wrapper, rejection/drop,
  worker failure, and stale-generation cleanup.
- Run focused CacheKit tests and commit this slice independently.

### Slice 2: reflective streaming bridge

- Teach `StatePrefetcher` to discover only the completion-bearing hook and to
  return an explicit unsupported/failure result rather than an immediate-ready
  success when no eligible wrapper exists.
- Preserve the current legacy `prefetch` behavior.
- Unit-test supported, unsupported, reflection failure, selector failure, and
  multi-state aggregation.
- Run focused streaming tests and commit this slice independently.

### Slice 3: ordered bounded ring and availability

- Add a feature-gated two-slot ring to `StreamRecordBatchOutput`.
- Seal a batch, start completion-bearing prefetch, and continue filling while
  capacity remains. Drain ready head batches in mailbox order.
- Extend the processor/output contract so ring-full pending heads return
  `NOTHING_AVAILABLE` plus a completion/timeout availability future. Never block
  the mailbox thread.
- Fence all control events and lifecycle transitions; cancel outstanding
  reservations/futures on fallback and teardown.
- Test thread identity, strict sequence, overlap, ring-full suspend/resume,
  failure/rejection/timeout/missing state/selector/generation fallbacks,
  watermarks/status/latency/end, cancellation/disposal, object reuse, and the
  feature-off legacy paths.
- Run focused streaming and CacheKit suites and commit this slice independently.

### Slice 4: configuration and observability

- Add documented opt-in config with default `false`, max in-flight default 2
  and legal range 2..4, and a bounded timeout.
- Expose `readyBeforeDispatch`, `prefetchWaitNanos`,
  `dispatchBeforeReadyFallbacks`, in-flight depth/ring-full duration,
  queue/service time, staged consumed/discarded, authoritative reads avoided,
  and retained bytes/records.
- Verify default-off compatibility and exact config diffs, then commit.

## Advancement gates

1. All focused correctness tests pass and the candidate archive/source identity
   is independently verified.
2. Run a same-source x86 q9 control/candidate canary at 100M with 8 TMs / 16
   slots and the existing CPU/ownership gates.
3. Require real ring overlap and completion-before-dispatch evidence, no fallback
   path anomaly, and at least `+3.00%` K/s/core before a 15-query R1.
4. Only a valid 15-query R1 winner advances to R2/R3 replication. Otherwise
   record the rejection and stop.

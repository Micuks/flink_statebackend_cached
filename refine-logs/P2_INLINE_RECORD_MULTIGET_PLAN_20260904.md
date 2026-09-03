# P2 inline record-key MultiGet plan

## Trigger

Execute this implementation only if the paired Kunpeng ready-gate depth-four screen is
non-positive.  The attempt-3 profile shows that ready gating avoids point reads but pays about
721 us of worker queue time for about 29 us of worker service, in addition to mailbox parking and
record retention.  Increasing queue depth cannot remove that queueing boundary.

## Hypothesis

For the exact record batch selected for ordinary arrival-order replay, extract and deduplicate its
keys on the mailbox thread and issue one synchronous RocksDB MultiGet immediately before replay.
This removes the asynchronous worker queue, completion futures, ready ring, timeouts, and retained
record references while preserving the point-read fallback for cached keys, small miss sets,
unsupported states, or any failure.

## Correctness and isolation

- Add a default-off `state.backend.cachekit.bp-prefetch.immediate-record.enabled` setting.
- The immediate-record path is mutually exclusive with async chunks and ready gating.
- Keep dispatch order and the existing authoritative `value()` path unchanged.
- Broadcast record keys only to access-observed `CachedInternalValueState` wrappers whose namespace
  is `VoidNamespace`; never guess a window/session namespace and never include MapState.
- Require at least `multiGetMinBatchSize` unresolved keys.  Do not replace a small miss set with
  synchronous point Gets; ordinary replay remains authoritative for those keys.
- Catch optional-hook failures and dispatch every record exactly once.
- Do not attribute unchanged chain copy-elision, LocalPreagg, native request-plane, or cache settings
  to this candidate.

## Activation evidence

Expose runtime attempts/handled batches and backend record-immediate MultiGet batch/key/staged
counters.  A query is effective only when the independent audit proves the path active, at least
one backend MultiGet executed and staged values were subsequently consumed, with zero worker
failure/admission-error evidence.

## Verification sequence

1. Add streaming-runtime tests for feature-off behavior, pre-dispatch ordering, unsupported/failing
   hook fallback, exactly-once arrival-order replay, and ready/async mutual exclusion.
2. Add `StatePrefetcher` tests for key extraction, reflective hook result handling, and failure
   closure.
3. Add CacheKit tests for access guidance, `VoidNamespace` filtering, MultiGet-only minimum, staged
   value correctness, and stale-generation fallback.
4. Run focused module tests, formatting/checkstyle, and package the same source for x86_64 and
   AArch64.
5. Run a short paired q9 screen on an idle NUMA cluster.  Continue only if activation is valid and
   K/s/core is positive; otherwise use its profile/counters to revise or reject the path.
6. Confirm effective queries with fresh same-host control/candidate pairs and compute the arithmetic
   mean of per-query K/s/core uplifts.  Stop only when that mean is greater than 10% and all legs and
   activation audits are valid.

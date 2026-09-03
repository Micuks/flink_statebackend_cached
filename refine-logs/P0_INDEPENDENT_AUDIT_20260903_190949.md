# P0 independent audit

Audit scope: read-only verification of the completed x86 q9 shape screen and its
local evidence mirror. The auditor did not modify the worktree.

## Verdict

- Evidence integrity: `PASS`.
- Performance advancement gate: `FAIL`.
- P0 disposition: `REJECT`; do not run the 15-query P0 campaign.
- Next authorized stage: P1 ready-gated prefetch implementation.

## Verified evidence

- Runtime source is `7a9e568dcbd75ab4e24e7a33a4339f5cd1cf8000`; the frozen
  overlay source is `bbd39ab`; the deployed candidate JAR SHA-256 is
  `e6bcbb6ea5a7e39158bc5ccff67a0e59b9d7d6b8d7eaf205bbb52b0ce8b7938f`
  and its size is 3,089,321 bytes.
- Local and remote identity, config, runner, summarizer, and audit hashes match.
  The 72-file measurement workspace manifest also matches exactly.
- Exactly six raw logs and six completion markers exist, with no failed-leg
  markers. Every leg has a valid warmup, job completion, and `LEG_COMPLETE`
  marker, and no validation error, fatal error, or OOM evidence.
- Each leg has 46 CPU samples and 8/8 TaskManager coverage. Maximum observed
  aggregate cores are 15.86, below the 16.05 bound. Recomputed raw throughput
  divided by cores agrees with the reported per-core result within 0.029%.
- No checkpoint interval is enabled. A config-diff recomputation found only the
  four declared treatment axes; async cancellation remains disabled everywhere.
- CPU receiver ownership is valid: two receiver roots cover eight distinct
  TaskManager sender JVMs across the two TaskManager containers.
- The x86 project was fully torn down after the campaign; its containers and
  project volumes are absent.

## Performance conclusion

All six legs are valid. The five treatment deltas versus the same-source control
are `-0.95%`, `-1.03%`, `-0.84%`, `-1.61%`, and `-0.55%`. The best treatment,
`c32-g16-d32`, therefore misses the predeclared `+3.00%` advancement threshold.
The auditor independently recomputed the paired arithmetic; deviations from the
rounded report are at most 0.03 percentage points.

Async worker activation is proven in every treatment and worker build/failure
counters are zero, but small-chunk drops become substantially worse. The
`asyncUsefulValues` counter is zero in every leg and is not useful for this
decision. The result exhausts P0 and authorizes P1.

## Audit limitation

The runner's uninterrupted leg progression and final cleanup support host
isolation, but the campaign did not persist a positive `docker ps` snapshot for
each individual leg. Future campaigns should save one in every leg evidence
directory.

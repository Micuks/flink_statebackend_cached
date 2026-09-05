# P20r2 continuation audit

The interrupted P20 effective-five run stopped during q5 B. The SSH session and remote
runner were absent on 2026-09-05 at 10:53 +08:00. Its B leg lacks LEG_COMPLETE and is
not accepted. No existing campaign process was restarted while live.

Two source correctness gaps were repaired in `6cbdc913863ac999702a308aa6d17200513fa9b4`:

- `keys()` and `values()` now route through mutation-aware entries when the memo is enabled.
- A null get is memoized separately from proven absence. It cannot answer contains().
  putAll null values retain presence, and exposing a delegate incremental visitor disables
  subsequent point memo use because visitor writes bypass wrapper hooks.

CachedInternalMapStateTest: 47 tests, zero failures/errors/skips. The two added regression
tests exercise nullable presence, keys/values removal, and visitor fallback.

Deployment audit also found P20 compose mounting `/tmp/ckkp5a9p19e5n0` while the P20
runner cleaned and inventoried a different scratch. Consequently the old q11 +23.31%
canary is exploratory only, not final acceptance evidence. P20r2 has an independently
validated mount `/tmp/ckkp20r2e5n0` for JM and both TM containers, matching runner cleanup
and SST inventory. Existing evidence remains intact.

P20r2 artifacts overlay only CachedInternalMapState class entries onto frozen P19 jars.
Entry sets and all non-overlay bytes were checked equal to the P19 bases.

- x86 SHA256: `8867c092330ae54ad8967a7fc9d4cf954d7477e1cec73845082ab99742bed68f`
- ARM SHA256: `abe350c9f4d46cd5afc3c497f8f698c3dad5ddc170b75aa4563522321051f887`
- Remote campaign: `/home/wuql/flink-cluster/experiments/cachekit-p20r2-point-memo-effective5-100m-kunpeng-numa0-20260905`
- Compose project: `ckkp20r2e5n0`; REST 10942, Prometheus 12131, Pushgateway 12132.
- 100M events; q5/q9/q11/q15/q18; fresh hot2 A and A+P19+P20 pairs, same artifact.
- NUMA0, 8 TM JVMs / 16 slots; five-second foreign CPU overlap monitor.

Acceptance remains pending. The arithmetic mean includes every query with actual useful
P19 or P20 hits, including regressions; inactive queries are reported separately.

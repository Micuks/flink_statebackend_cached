# CacheKit CDC correctness evidence index

## Confirmed historical evidence

The repository history contains a completed 15-query adaptive bp-prefetch CDC
gate, not merely a planned check:

- `results-bp-prefetch-cdc-adaptive-15q-combined/STATUS` contains `PASS`.
- Its `diff.txt` records 15/15 PASS: eight sorted-output passes, six
  CDC-collapse passes, and q12 through a deterministic guarded first-window
  processing-time oracle.
- The durable summary was committed as
  `docs/BP-PREFETCH-FINAL-REPORT-ZH.md`; CDC runner support includes commit
  `30d469d2763a377aceb943090c72476b320c78c0`.
- The evidence summary commit `6806da57c4380fafb147df6049b8b1c0996ee56c`
  is an ancestor of Bloom source `5e39087b51124b3c9486b54b8035460233523528`,
  MultiGet candidate `6ab859944d4190ffc49d0be3d734e87c53f7cf64`, and the
  current `wuql/cachekit/dev` line used to publish this index.

Separately, `results-e2b-cdc-correctness/diff.md` records 15/15 PASS for the
E2b capacity comparison at 100k events. It is useful historical cache-path
correctness evidence, but it is not an exact-binary proof for every later
performance candidate.

## Scope boundary

CDC correctness and performance validity are different claims. The evidence
above proves the historical CacheKit/adaptive prefetch path passed the named
CDC oracle. It does **not** retroactively certify every later overlay or native
binary. In particular:

- MultiGet `6ab859...` has strong unit/mechanism and completed-job evidence in
  its experiment bundle, but this index does not claim a separate exact-binary
  15q CDC rerun for that candidate.
- Bloom filters are false-positive-only lookup gates and should not alter state
  semantics, but the Bloom performance campaigns are validated by completed
  Nexmark jobs and config/hash audits, not by a newly run exact-binary CDC
  matrix.
- FullOpt contains multiple already-tested components; the listed historical
  CDC pass supports ancestry and component safety, while the exact FullOpt+
  Bloom binary would require its own CDC matrix for a release-grade statement.

## Local evidence paths

- Durable copies committed with this index:
  [`cdc-evidence/CDC_15Q_STATUS.txt`](cdc-evidence/CDC_15Q_STATUS.txt) and
  [`cdc-evidence/CDC_15Q_DIFF.txt`](cdc-evidence/CDC_15Q_DIFF.txt).
- `results-bp-prefetch-variants/CDC-15Q-COMBINED-ZH.md`
- `results-bp-prefetch-cdc-adaptive-15q-combined/STATUS`
- `results-bp-prefetch-cdc-adaptive-15q-combined/diff.txt`
- `results-bp-prefetch-cdc-adaptive/diff.txt`
- `results-bp-prefetch-cdc-q12-proctime-guarded-firstwindow-offon-12k-tps1000/diff.txt`
- `results-e2b-cdc-correctness/diff.md`

This index deliberately distinguishes “evidence exists and is reachable” from
“the exact later performance artifact was rerun through CDC.”

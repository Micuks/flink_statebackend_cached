# P10 Effective-5 Promotion Plan

This promotion is conditional on a valid q9 P10 screen with nonzero snapshot-maintenance and
single-entry short-circuit counters. The predeclared 64K source contrast
`hot2-maintained-64k` versus `hot2-overlay-64k` must be positive, and the integrated
`hot2-maintained-64k` treatment must improve over the fresh `baseline`, before expansion.

## Claim boundary

The headline will be calculated entirely within cloud x86 host 114. It will not substitute the
x86 q9 result into the Kunpeng P7 table. Kunpeng remains a separate confirmation campaign if its
existing SSH control socket becomes available again.

Archived P4 controls remain useful only for fast plausibility checks:

| Query | Frozen control K/s/core | Evidence |
|---|---:|---|
| q5 | 95.83 | P4 x86-effective3 control |
| q9 | 27.23 | P4 x86 paired q9 control |
| q11 | 68.14 | P4 x86-effective3 control |
| q18 | 115.95 | P4 x86-effective3 control |

They cannot support the final mean: an exact config audit found that P4 used 1,024 MiB RocksDB
memory per slot, while the P10 matrix uses 2,048 MiB plus
`SPINNING_DISK_OPTIMIZED_HIGH_MEM`. The final table therefore uses fresh, same-host, same-artifact
legs for every query. No P4 or Kunpeng result is substituted into the headline.

## Candidate legs

- `stage_effective5_x86.sh` refuses to stage unless the sealed q9 64K source contrast and q9
  integrated uplift are both positive. `summarize_effective5.py` then joins the q9 screen with the
  four-query promotion campaign and recomputes every uplift from raw leg results.
- Reuse the valid q9 `baseline`, `hot2-a`, and `hot2-maintained-64k` P10 screen legs.
- For q5, q11, q15, and q18, run all three variants with the same P10 artifact:
  - `baseline`: hot levels 0, MapState value cache off, overlay off, maintenance off;
  - `hot2-a`: hot levels 2, MapState value cache off, overlay off, maintenance off;
  - `hot2-maintained-64k`: hot levels 2, MapState value cache 65,536, overlay on, maintenance on,
    exact-membership capacity 65,536.

Every candidate leg must use 8 TaskManagers, 16 slots, 100M events, the same storage mount, valid
CPU ownership, P10 artifact SHA-256
`09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251`, and explicit overlay /
snapshot-maintenance environment gates. The only baseline-to-A config difference is hot-level
count; the only A-to-A+B differences are the declared MapState cache and source gates. Activation
is reported per query rather than assumed:
queries without applicable MapState work may legitimately show zero mechanism counters, but they
remain in the mean.

## Gate

For each query, report both `(A / baseline - 1) * 100` and `(A+B / baseline - 1) * 100`, plus the
incremental `(A+B / A - 1) * 100`. The headline is the arithmetic mean of the five integrated
A+B-versus-baseline percentages, with no query removed after observation. Promotion requires all
15 valid legs, a positive directly isolated 64K source contrast on q9, and integrated mean uplift
greater than or equal to 10.00%.

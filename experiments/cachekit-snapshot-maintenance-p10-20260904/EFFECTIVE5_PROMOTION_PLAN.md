# P10 Effective-5 Promotion Plan

This promotion is conditional on a valid q9 P10 screen with nonzero snapshot-maintenance and
single-entry short-circuit counters. A source mechanism that does not improve the
`hot2-maintained` versus `hot2-overlay` contrast is rejected before expansion.

## Claim boundary

The headline will be calculated entirely within cloud x86 host 114. It will not substitute the
x86 q9 result into the Kunpeng P7 table. Kunpeng remains a separate confirmation campaign if its
existing SSH control socket becomes available again.

The frozen x86 P4 controls that can be reused are:

| Query | Frozen control K/s/core | Evidence |
|---|---:|---|
| q5 | 95.83 | P4 x86-effective3 control |
| q9 | 27.23 | P4 x86 paired q9 control |
| q11 | 68.14 | P4 x86-effective3 control |
| q18 | 115.95 | P4 x86-effective3 control |

All four controls use the same host, 100M events, frozen P4 x86 artifact SHA-256
`66f1bac9c25c6e8a777b2ca370e2b21621cccc57e020d869db2fd7c245c6a46b`, and valid CPU
accounting. There is no identity-compatible frozen x86 q15 P4 control in the archived evidence,
so q15 must receive a fresh control leg; it must not be filled from Kunpeng or from an older
incompatible RDB campaign.

## Candidate legs

- Reuse the valid q9 `hot2-maintained` P10 screen leg.
- Run P10 `hot2-maintained` for q5, q11, q15, and q18 on the idle x86 host.
- Run a fresh q15 P4 control config on the same host. The source artifact may be P10 only if both
  source gates and MapState cache are explicitly off and the rendered P4 configuration matches;
  otherwise rebuild/use the frozen P4 artifact.

Every candidate leg must use 8 TaskManagers, 16 slots, 100M events, the same storage mount, valid
CPU ownership, P10 artifact SHA-256
`09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251`, and explicit overlay /
snapshot-maintenance environment gates. Activation is reported per query rather than assumed:
queries without applicable MapState work may legitimately show zero mechanism counters, but they
remain in the mean.

## Gate

For each query, compute `(candidate K/s/core / control K/s/core - 1) * 100`. The headline is the
arithmetic mean of all five percentages, with no query removed after observation. Promotion
requires all five valid legs and mean uplift greater than or equal to 10.00%.

# P16 set-associative snapshot authority

P16 preserves the multi-row coverage that made P14 the best q9 candidate (+8.29%) while removing
the general access-ordered snapshot LRU from direct `get` and `contains`. Complete value-bearing
tiny-map rows are also published into a fixed four-way set-associative table. Each slot stores a
precomputed key/namespace hash, so a miss performs one hash plus four integer checks; hits compare
the exact copied key and namespace before using the authoritative values.

The table has at least the configured 2,000-row capacity, uses per-set round-robin replacement,
and is invalidated by every existing mutation, EMPTY/key-only replacement, and `clear` path. The
general MapSnapshot LRU remains unchanged for iterator short-circuits. This combines P14's useful
multi-row coverage with P15's separation of point reads from LRU access-order bookkeeping.

The q9 screen uses fresh same-artifact A/A+B legs on Kunpeng NUMA0. Activation and the 10% K/s/core
promotion gate are identical to P14/P15; a passing q9 screen is expanded to the effective five.

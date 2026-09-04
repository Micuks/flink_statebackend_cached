# P11 Direct Exact MapState Snapshot Maintenance

P10 proved that incrementally maintaining an exact small-MapState membership snapshot is active,
but combining it with the dirty overlay costs 17.15% versus hot2 A on Kunpeng q9. P11 removes
that rejected dependency: the default-off maintenance gate updates the existing snapshot cache
after ordinary write-through `put`, `putAll`, and `remove` operations.

The q9 source screen uses one P11 artifact in every leg:

- `hot2-a`: P7 hot-level compression, snapshot cache 2K, direct maintenance disabled.
- `hot2-direct-maintained`: A plus direct maintenance, snapshot cache 2K.
- `hot2-direct-maintained-64k`: A plus direct maintenance, snapshot cache 64K.

Dirty overlay and Java MapState value cache stay disabled in all legs. The screen requires valid
100M-event measurements, exact artifact/config identity, nonzero maintenance put attempts and
applied updates in both treatment legs, no dirty-overlay marker, and no fatal runtime errors.
The predeclared screen selection rule chooses the higher valid q9 K/s/core result of the 2K and
64K capacities. Promotion requires at least 10% K/s/core uplift over the fresh same-artifact A leg. A promoted
effective-query campaign must rerun fresh A/A+B controls on one host and compute the arithmetic
mean of per-query percentages.

# P8 indexed MapState write delta

## Question

Can a source-level pending-write delta complement P7 hot-level compression by reducing q9's
RocksDB point-write and read-after-write overhead without changing the durable state format?

## A + B

- A: P7 `state.backend.rocksdb.compression.uncompressed-hot-levels: 2` with Snappy on colder
  levels.
- B: opt-in `state.backend.rocksdb.write-batch-with-index.enabled: true`.
- A control: the same P8 artifact with B disabled.
- A+B treatment: the same P8 artifact with B enabled.

B keeps MapState point puts/deletes in a bounded `WriteBatchWithIndex`, uses indexed point reads
and merged iterators for read-your-writes, and flushes before bulk reads, clear, snapshot,
savepoint, key enumeration, migration, and entry counting. The ordinary shared `WriteBatch`
remains separate for priority queues and other existing users. The option defaults to false.

## Screen and advancement

1. Run q9 on cloud x86 using the same P8 artifact for A and A+B.
2. Require completed jobs, valid CPU accounting, exact config diffs, and terminal activation
   counters. A+B must have nonzero MapState puts, indexed reads or iterators with pending writes,
   and more than one flushed MapState entry per flush on average.
3. If A+B is effective against A, run q5/q9/q11/q15/q18 on Kunpeng. Reuse frozen RocksDB and P7
   baselines for fast contextual validation, but calculate B's causal uplift only against the
   same-artifact A legs.
4. Report per-query raw K/s, cores, K/s/core, valid-leg status, and the arithmetic mean of
   per-query K/s/core uplifts. The completion gate is mean uplift greater than 10% across the
   effective queries.

Cross-host absolute throughput is never pooled. A foreign job is acceptable only when its CPU and
NUMA allocation is disjoint from the experiment allocation and the preflight evidence is saved.

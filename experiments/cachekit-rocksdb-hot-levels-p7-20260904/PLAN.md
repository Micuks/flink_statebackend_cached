# P7 source-level hot-level compression policy

## Question

Can a source-level RocksDB policy retain most of P6's write-path uplift without leaving every SST
level uncompressed?

## Mechanism

`state.backend.rocksdb.compression.uncompressed-hot-levels` is an opt-in Flink source policy. It
maps the first N RocksDB compression-policy slots to `NO_COMPRESSION` and leaves later slots on
`state.backend.rocksdb.compression.type`. The default is zero and preserves the previous single
compression policy.

With RocksDB dynamic level sizing, policy slot zero controls L0 and slot one follows RocksDB's
base-level-relative semantics. The first screen therefore tests one and two uncompressed hot
levels, both with Snappy retained for colder levels.

## Screen

- Host: idle cloud x86, fixed 16-vCPU topology.
- Query: Nexmark q9, 100 million events, checkpointing disabled.
- Same-source variants: HIGH_MEM + Snappy with hot-level counts 0, 1, and 2.
- Primary comparison: each treatment against the same-artifact hot-level-count-zero control.
- Secondary comparison: frozen P6 HIGH_MEM global-no-compression q9 reference on the same host.
- Gate: real job completion, 8 TM coverage, cores at most 16.05, exact runtime policy logs, and
  non-empty live/post-leg SST evidence.

The strongest valid source policy advances to the five effective Kunpeng queries. Cross-host
absolute throughput is never pooled.

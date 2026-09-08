# CacheKit single-fat-JAR delivery contract

User requirement (2026-09-08): deliver **one CacheKit fat JAR** for CacheKit
optimizations, including FullOpt+LC. Config, usage and audit files may accompany
it; do not require additional patched flink-dist, table uber/runtime or bridge
JARs as the CacheKit delivery. The existing compatible Flink distribution,
benchmark driver and platform dependencies remain prerequisites.

## Packaging

- Include CacheKit code, its bundled runtime dependencies and the complete
  classes required to override Flink behavior, including nested classes.
- P30 requires BinaryStringData.class and BinaryStringData$*.class from the
  patched flink-table-common module. P29 requires the batch interface and
  RocksDBValueState classes. Preserve the Streaming/Table Runtime overlays.
- Overriding means selecting a whole same-package/same-name class before the
  original in the relevant classloader, not patching an individual method or
  hot-replacing an already loaded class. Restart all owned JVMs.
- Use a clean, ordered build of patched sibling modules so stale Maven artifacts
  or unpack markers do not silently package old code.
- Keep the CacheKit-owned gate names:
  cachekit.binary-string.lazy-copy.enabled /
  CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED. Default OFF.

## Deployment and acceptance

1. Deploy the single artifact as 00-cachekit-fullopt-lc.jar in the compatible
   Flink lib directory, or explicitly prepend it to the effective system
   classpath. Verify the actual launcher order, not filename assumptions alone.
2. Do not retain another CacheKit version or separately supplied patch JARs that
   compete for the same classes. Audit before changing files; preserve unrelated
   artifacts and jobs. No automatic deletion of existing runtime files.
3. Verify required classes and nested classes are packaged and bundled dependency
   namespaces are present. Record commit, JAR SHA256 and exact rendered config.
4. Verify defining classloader and CodeSource for the override classes in the
   actual JM/TM loading paths. Parent/child or planner loaders can differ; a
   successful local system-classloader check alone is not cluster evidence.
5. Check LC ON/OFF behavior and P29 API linkage, then deployment activation if
   running a job is in scope. Do not claim performance from packaging tests.

## Current implementation and historical boundary

The CacheKit module POM packages BinaryStringData alongside its existing
Streaming/RocksDB/Table Runtime overlays. The source repository's
reproduction/fullopt-lc/build_fat_jar.py builds one delivery JAR and performs
local class-origin and copy-semantics checks. reproduction/fullopt-lc/rebuild.py
defaults to this single-JAR path.

The earlier three-JAR archive reconstruction is **legacy evidence tooling only**,
available only through explicit --legacy-multi-jar. It does not meet this delivery
contract and must not be presented as the default solution. Preserve historical
result/artifact identities; a newly packaged single JAR is a new runtime identity.
The historical +65.48% is not a newly measured single-JAR result.

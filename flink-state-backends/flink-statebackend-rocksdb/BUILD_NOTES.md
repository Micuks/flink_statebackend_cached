<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# CacheKit DSTL RocksDB JNI packaging

The CacheKit DSTL lane intentionally keeps the custom coordinate
`com.ververica:frocksdbjni:6.20.3-ververica-1.0-cachekit-dstl1`. Do not change
the POM coordinate to distinguish x86-64 and AArch64 builds.

Treat the JNI payload as host/architecture specific unless the artifact audit
proves that one byte-identical JAR contains the required native payloads for
both architectures. Build, test, and package the x86-64 distribution on the
x86-64 host and the Kunpeng distribution on the AArch64 host. Never copy a
single-architecture `librocksdbjni` payload into the other distribution.

For every packaged distribution, preserve this audit:

1. Record `uname -m`, the source commit, the custom JAR SHA-256, and the final
   Flink module/distribution SHA-256.
2. List the JAR's embedded `librocksdbjni` resources. Extract them into a
   temporary directory and record `file` plus `readelf -h` output for each ELF.
3. Run `RocksDBDirectValueAccessTest` and `RocksDBDirectArenaProbeTest` on the
   target host before packaging. The probe is a correctness/per-call timing
   smoke test; Nexmark remains the performance gate.
4. Verify the packaged distribution resolves the audited custom JAR and not a
   stale artifact from another Maven cache.

Identical cross-host JAR SHA-256 values are useful reproducibility evidence,
but the filename, Maven coordinate, or hash alone is not proof of a universal
JNI payload. Call the artifact multi-architecture only when the embedded ELF
inventory and successful target-host tests establish that fact.

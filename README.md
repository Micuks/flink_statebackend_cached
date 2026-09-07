# Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)


### Features

* A streaming-first runtime that supports both batch processing and data streaming programs

* Elegant and fluent APIs in Java and Scala

* A runtime that supports very high throughput and low event latency at the same time

* Support for *event time* and *out-of-order* processing in the DataStream API, based on the *Dataflow Model*

* Flexible windowing (time, count, sessions, custom triggers) across different time semantics (event time, processing time)

* Fault-tolerance with *exactly-once* processing guarantees

* Natural back-pressure in streaming programs

* Libraries for Graph processing (batch), Machine Learning (batch), and Complex Event Processing (streaming)

* Built-in support for iterative programs (BSP) in the DataSet (batch) API

* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms

* Compatibility layers for Apache Hadoop MapReduce

* Integration with YARN, HDFS, HBase, and other components of the Apache Hadoop ecosystem


### Streaming Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.socketTextStream(host, port, '\n')

val windowCounts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .keyBy("word")
  .window(TumblingProcessingTimeWindow.of(Time.seconds(5)))
  .sum("count")

windowCounts.print()
```

### Batch Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.readTextFile(path)

val counts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .groupBy("word")
  .sum("count")

counts.writeAsCsv(outputPath)
```



## Building Apache Flink from Source

Prerequisites for building Flink:

* Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
* Git
* Maven (we recommend version 3.2.5 and require at least 3.1.1)
* Java 8 or 11 (Java 9 or 10 may work)

```
git clone https://github.com/apache/flink.git
cd flink
./mvnw clean package -DskipTests # this will take up to 10 minutes
```

Flink is now installed in `build-target`.

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.1.1 creates the libraries properly.
To build unit tests with Java 8, use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*

## Reproducing the Kunpeng wholekey1 result

The historical wholekey1 result is tied to the complete runtime bundle, not to
the CacheKit JAR alone.  Use commit `b7d408b0a7504a114ccb56fc49ed40ddc6ddeefd`
(`b7d408b`) and the AArch64 image
`nexmark-bench-v2:kunpeng-native-p6-batch-8c93daacba` on a Kunpeng 920 host.
The image uses openEuler 22.03-LTS-SP2, Flink 1.16.3 and Nexmark 0.3
(`6b3646c3baec701f1fa74baf938d235f742e5d3c`).

The exact benchmark configuration is checked in at
[`conf/flink-conf-cachekit-wholekey1-reproduction.yaml`](conf/flink-conf-cachekit-wholekey1-reproduction.yaml)
(SHA-256 `830dae668d6936859f6edf1df2f3e53f9fdb4bc66ffbe378ab0856ce1a96e2f3`).
It enables the CacheKit value/snapshot/prefetch paths, keeps MapState point and
presence caches off, and explicitly disables ListState COW/RYW and PriorityQueue
optimizations.  The run uses 100,000,000 events, parallelism 16, two task slots,
no warm-up and no checkpoints (`CHECKPOINT_INTERVAL_MS=0`).

### Required runtime artifacts

Do not substitute a current Flink distribution or a separately built
`frocksdbjni` library when reproducing the historical number.  The original
wholekey1 native implementation is embedded in the AArch64 member
`librocksdbjni-linux-aarch64.so` of the historical `flink-dist` JAR; there was
no standalone wholekey1 JAR in this bundle.  Copy these files from the archived
campaign directory
`/home/wutb/nexmark-bench-v2/runtime/cachekit-fullopt-wholekey-15q-100m-1r-20260831/override/lib/`
into the benchmark override directory and verify their hashes:

| file | SHA-256 |
| --- | --- |
| `flink-statebackend-cachekit-1.16-SNAPSHOT.jar` | `d9a95c5d7414864dc2af39cabf6669e1067f270341c6751824dca8c7f2ccd55b` |
| `flink-dist-1.16.3.jar` (contains wholekey1; embedded member SHA-256 `b45c26346bba50b49fea5e75b56471d324d74b2995fedb6e81758cde35200dfa`) | `a635cedad7c2b9a5934c32e22a0407719078baedc68a11bfef0629f8c5dcf7ba` |
| `flink-table-runtime-1.16.3.jar` | `080473c5148183b516f05eb9d01cdec530a7f94beb5154a46e92a28a6618be17` |
| `fastutil-8.5.12.jar` | `b5543ee08d062d551cf0a5c9bc0fb70588b0382079029ba48941fa9c9be8a5d4` |

For a source-level CacheKit rebuild at this commit, compile only the CacheKit
module and replace the corresponding JAR in the override directory:

```bash
git checkout b7d408b0a7504a114ccb56fc49ed40ddc6ddeefd
./mvnw -pl flink-state-backends/flink-statebackend-cachekit -am -DskipTests package
CAMPAIGN=/home/wutb/nexmark-bench-v2/runtime/cachekit-fullopt-wholekey-15q-100m-1r-20260831
cp flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
  "$CAMPAIGN/override/lib/"
```

This rebuild is useful for source validation, but it is not bit-for-bit
equivalent to the archived CacheKit JAR above; use the archived JAR when the
historical throughput percentage itself is the acceptance criterion.

If the archived JARs are unavailable, the source checkout can still be built
with the normal CacheKit module-only build, but the historical throughput
percentage is not reproducible until the matching Flink distribution and its
embedded wholekey1 library are rebuilt or restored.

### Launch command

The Nexmark harness is maintained outside this repository.  Set `CAMPAIGN` to
the directory containing the image's `nexmark-no-warmup.yaml` and the verified
override JARs, then run:

```bash
ROOT=/home/wutb/nexmark-bench-v2
CACHEKIT_ROOT=/home/wutb/code/flink_statebackend_cached
CAMPAIGN=$ROOT/runtime/cachekit-fullopt-wholekey-15q-100m-1r-20260831

export IMAGE_REF=nexmark-bench-v2:kunpeng-native-p6-batch-8c93daacba
export COMPOSE_PROJECT_NAME=ckfullopt-wholekey1-repro
export USE_DOCKER_RUN_COMPOSE=true
export REST_PORT=9305
export JM_SSH_PORT=2560
export TM1_SSH_PORT=2561
export TM2_SSH_PORT=2562
export JM_CPUSET=160,162,164,166,168,170,172,174
export TM1_CPUSET=176,178,180,182,184,186,188,190
export TM2_CPUSET=192,194,196,198,200,202,204,206
export CHECKPOINT_INTERVAL_MS=0
export DATA_ROOT_BASE=/tmp/nexmark-bench-v2-runtime-data
export NEXMARK_CONFIG=$CAMPAIGN/conf/nexmark-no-warmup.yaml

"$ROOT/bin/launch_benchmark.sh" \
  --backend cachekit \
  --conf "$CACHEKIT_ROOT/conf/flink-conf-cachekit-wholekey1-reproduction.yaml" \
  --override-dir "$CAMPAIGN/override" \
  --queries q4,q5,q8,q9,q11,q18,q19,q20,q3,q7,q12,q13,q15,q16,q17 \
  --events 100000000 --rounds 3 --timeout 10800 \
  --label cachekit-fullopt-wholekey1-reproduction
```

Keep the three containers on the listed, non-overlapping even CPUs (node 2),
close any competing benchmark, and start a fresh Compose project for each
query campaign.  Report throughput as events per second per core from the
generated `summary.csv`.

The reference campaign completed all 15 queries for three CacheKit rounds and
three RocksDB rounds.  Its arithmetic-mean uplift was **49.53%** (the earlier
historical record is 49.71%); a one-round recheck was **50.10%**.  These values
are a reproducibility reference for this fixed image, artifact set, CPU binding,
configuration and input size—not a guarantee for a different runtime or host.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala


### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

### Eclipse Scala IDE

**NOTE:** From our experience, this setup does not work with Flink
due to deficiencies of the old Eclipse version bundled with Scala IDE 3.0.3 or
due to version incompatibilities with the bundled Scala version in Scala IDE 4.4.1.

**We recommend to use IntelliJ instead (see above)**

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.


## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).


## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.

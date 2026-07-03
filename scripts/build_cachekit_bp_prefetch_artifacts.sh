#!/usr/bin/env bash
# Build the modified CacheKit/Flink modules and stage the runtime jars into the
# shared Nexmark benchmark workspace.
set -euo pipefail

WT=${WT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}
BENCH_ROOT=${BENCH_ROOT:-/mnt/data2/wuql/flink-cluster}

cd "$WT"

./mvnw -pl flink-streaming-java \
    -am -Dmaven.test.skip=true -DskipTests -DskipITs -Drat.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true package
./mvnw -pl flink-table/flink-table-runtime \
    -Dmaven.test.skip=true -DskipTests -DskipITs -Drat.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true package
./mvnw -pl flink-state-backends/flink-statebackend-cachekit \
    -Dmaven.test.skip=true -DskipTests -DskipITs -Drat.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true package

DIST_JAR="$BENCH_ROOT/lib/flink-dist-1.16.3.jar"
DIST_BACKUP="$BENCH_ROOT/lib/flink-dist-1.16.3.jar.before-cachekit-bp"
if [ ! -f "$DIST_BACKUP" ]; then
    mkdir -p "$(dirname "$DIST_BACKUP")"
    if [ -f "$DIST_JAR" ]; then
        cp "$DIST_JAR" "$DIST_BACKUP"
    elif [ -f "$BENCH_ROOT/flink.tgz" ]; then
        tmp_dist_dir=$(mktemp -d)
        tar -xzf "$BENCH_ROOT/flink.tgz" -C "$tmp_dist_dir" flink-1.16.3/lib/flink-dist-1.16.3.jar
        cp "$tmp_dist_dir/flink-1.16.3/lib/flink-dist-1.16.3.jar" "$DIST_BACKUP"
        rm -rf "$tmp_dist_dir"
    else
        echo "No source dist jar found: $DIST_JAR or $BENCH_ROOT/flink.tgz" >&2
        exit 1
    fi
fi
cp "$DIST_BACKUP" "$DIST_JAR"
jar uf "$DIST_JAR" -C "$WT/flink-streaming-java/target/classes" org/apache/flink/streaming

install -D -m 0644 \
    "$WT/flink-table/flink-table-runtime/target/flink-table-runtime-1.16-SNAPSHOT.jar" \
    "$BENCH_ROOT/lib/flink-table-runtime-1.16.3.jar"
install -D -m 0644 \
    "$WT/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar" \
    "$BENCH_ROOT/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"

echo "Staged benchmark jars:"
ls -l \
    "$BENCH_ROOT/lib/flink-dist-1.16.3.jar" \
    "$BENCH_ROOT/lib/flink-table-runtime-1.16.3.jar" \
    "$BENCH_ROOT/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"

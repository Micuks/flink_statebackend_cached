# Ready-gated q9 async-profiler summary

## cpu

### control top inclusive frames

| Frame | Share |
| --- | ---: |
| `/usr/lib64/libc.so.6` | 84.40% |
| `Unsafe_Park` | 50.69% |
| `Parker::park` | 50.69% |
| `java/lang/Thread.run` | 48.82% |
| `jdk/internal/misc/Unsafe.park` | 47.68% |
| `pthread_cond_timedwait` | 39.43% |
| `pthread_cond_wait` | 37.21% |
| `java/util/concurrent/ThreadPoolExecutor.runWorker` | 36.75% |
| `java/util/concurrent/ThreadPoolExecutor$Worker.run` | 36.75% |
| `java/util/concurrent/locks/LockSupport.park` | 27.14% |
| `java/util/concurrent/ThreadPoolExecutor.getTask` | 26.53% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.await` | 17.57% |
| `java/util/concurrent/locks/LockSupport.parkNanos` | 16.75% |
| `os::PlatformEvent::park` | 16.12% |
| `java/util/concurrent/ForkJoinWorkerThread.run` | 15.56% |
| `java/util/concurrent/ForkJoinPool.runWorker` | 15.56% |
| `thread_native_entry` | 14.43% |
| `Thread::call_run` | 14.43% |
| `sun/nio/ch/SelectorImpl.select` | 14.29% |
| `sun/nio/ch/EPollSelectorImpl.doSelect` | 14.29% |
| `sun/nio/ch/SelectorImpl.lockAndDoSelect` | 14.29% |
| `sun/nio/ch/EPoll.wait` | 14.20% |
| `Java_sun_nio_ch_EPoll_wait` | 14.14% |
| `epoll_pwait` | 14.14% |
| `java/util/concurrent/LinkedBlockingQueue.take` | 13.64% |

### control rank scope

Share of all samples: 2.61%.

| Frame | Share within scope |
| --- | ---: |
| `/usr/lib64/libc.so.6` | 97.41% |
| `pthread_cond_timedwait` | 95.17% |
| `Unsafe_Park` | 61.88% |
| `Parker::park` | 61.88% |
| `org/apache/flink/runtime/taskmanager/Task.runWithSystemExitMonitoring` | 61.46% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.runMailboxLoop` | 61.46% |
| `org/apache/flink/runtime/taskmanager/Task.restoreAndInvoke` | 61.46% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.runMailboxLoop` | 61.46% |
| `org/apache/flink/runtime/taskmanager/Task.doRun` | 61.46% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.invoke` | 61.46% |
| `java/lang/Thread.run` | 61.46% |
| `org/apache/flink/runtime/taskmanager/Task.run` | 61.46% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.processMail` | 58.74% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.processMailsWhenDefaultActionUnavailable` | 58.73% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/TaskMailboxImpl.take` | 58.73% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.await` | 58.69% |
| `jdk/internal/misc/Unsafe.park` | 58.68% |
| `java/util/concurrent/locks/LockSupport.parkNanos` | 58.68% |
| `/usr/lib64/libstdc++.so.6.0.28` | 34.88% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$957/770569290.run` | 31.62% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$964/865178002.run` | 29.84% |
| `/cachekit-rocksdb/tmp/tm_192.168.64.6:42419-2c00dd/tmp/rocksdb-lib-fab6d5fcb6fe188038a447bdaf144f09/librocksdbjni-linux-aarch64.so` | 18.26% |
| `[Rank[13] -> Sin tid=2051]` | 17.99% |
| `/cachekit-rocksdb/tmp/tm_192.168.64.5:40885-76e35c/tmp/rocksdb-lib-d4dcddf44be38baf212af6c8a4a1a2b8/librocksdbjni-linux-aarch64.so` | 17.13% |
| `[Rank[13] -> Sin tid=2030]` | 16.88% |
| `[Rank[13] -> Sink: nexmark_q9[14] (10/16)#0 tid=2002]` | 16.77% |
| `[Rank[13] -> Sink: nexmark_q9[14] (1/16)#0 tid=1996]` | 16.61% |
| `[Rank[13] -> Sink: nexmark_q9[14] (11/16)#0 tid=2001]` | 15.89% |
| `[Rank[13] -> Sink: nexmark_q9[14] (7/16)#0 tid=1996]` | 15.84% |
| `[GC_active]` | 3.64% |

### control prefetch-worker scope

Share of all samples: 0.86%.

| Frame | Share within scope |
| --- | ---: |
| `/usr/lib64/libc.so.6` | 99.83% |
| `Unsafe_Park` | 99.50% |
| `Parker::park` | 99.50% |
| `pthread_cond_timedwait` | 97.25% |
| `java/util/concurrent/ThreadPoolExecutor.runWorker` | 94.19% |
| `java/util/concurrent/ThreadPoolExecutor$Worker.run` | 94.19% |
| `java/lang/Thread.run` | 94.19% |
| `java/util/concurrent/ThreadPoolExecutor.getTask` | 93.89% |
| `java/util/concurrent/ArrayBlockingQueue.poll` | 93.87% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.awaitNanos` | 93.87% |
| `jdk/internal/misc/Unsafe.park` | 93.86% |
| `java/util/concurrent/locks/LockSupport.parkNanos` | 93.86% |
| `[cachekit-bp-prefetch tid=1908]` | 51.64% |
| `[cachekit-bp-prefetch tid=1905]` | 48.36% |
| `[GC_active]` | 5.81% |
| `pthread_cond_wait` | 2.55% |
| `os::PlatformEvent::park` | 2.55% |
| `SafepointSynchronize::block` | 2.22% |
| `Monitor::lock_without_safepoint_check` | 2.22% |
| `SafepointMechanism::block_if_requested_slow` | 2.22% |
| `MemAllocator::allocate` | 0.34% |
| `G1CollectedHeap::attempt_allocation_slow` | 0.33% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$1TrackedPrefetchTask.run` | 0.25% |
| `MemAllocator::allocate_inside_tlab_slow` | 0.24% |
| `G1CollectedHeap::allocate_new_tlab` | 0.24% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunkIntoStaging` | 0.23% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.lambda$buildPreparedMultiGetTask$5` | 0.23% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunksIntoStaging` | 0.23% |
| `InstanceKlass::allocate_instance` | 0.22% |
| `CollectedHeap::obj_allocate` | 0.22% |

### ready-d2 top inclusive frames

| Frame | Share |
| --- | ---: |
| `/usr/lib64/libc.so.6` | 84.43% |
| `Unsafe_Park` | 51.28% |
| `Parker::park` | 51.27% |
| `java/lang/Thread.run` | 49.81% |
| `jdk/internal/misc/Unsafe.park` | 48.88% |
| `pthread_cond_timedwait` | 40.39% |
| `java/util/concurrent/ThreadPoolExecutor.runWorker` | 37.69% |
| `java/util/concurrent/ThreadPoolExecutor$Worker.run` | 37.69% |
| `pthread_cond_wait` | 36.37% |
| `java/util/concurrent/ThreadPoolExecutor.getTask` | 27.40% |
| `java/util/concurrent/locks/LockSupport.park` | 27.02% |
| `java/util/concurrent/locks/LockSupport.parkNanos` | 17.73% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.await` | 17.71% |
| `java/util/concurrent/ForkJoinPool.runWorker` | 15.76% |
| `java/util/concurrent/ForkJoinWorkerThread.run` | 15.76% |
| `os::PlatformEvent::park` | 15.68% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.awaitNanos` | 14.40% |
| `sun/nio/ch/SelectorImpl.lockAndDoSelect` | 14.37% |
| `sun/nio/ch/SelectorImpl.select` | 14.37% |
| `sun/nio/ch/EPollSelectorImpl.doSelect` | 14.37% |
| `sun/nio/ch/EPoll.wait` | 14.29% |
| `thread_native_entry` | 14.28% |
| `Thread::call_run` | 14.28% |
| `Java_sun_nio_ch_EPoll_wait` | 14.21% |
| `epoll_pwait` | 14.21% |

### ready-d2 rank scope

Share of all samples: 2.16%.

| Frame | Share within scope |
| --- | ---: |
| `/usr/lib64/libc.so.6` | 97.16% |
| `pthread_cond_timedwait` | 94.73% |
| `Unsafe_Park` | 75.74% |
| `Parker::park` | 75.74% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.runMailboxLoop` | 75.68% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.runMailboxLoop` | 75.68% |
| `org/apache/flink/runtime/taskmanager/Task.restoreAndInvoke` | 75.68% |
| `org/apache/flink/runtime/taskmanager/Task.doRun` | 75.68% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.invoke` | 75.68% |
| `java/lang/Thread.run` | 75.68% |
| `org/apache/flink/runtime/taskmanager/Task.runWithSystemExitMonitoring` | 75.68% |
| `org/apache/flink/runtime/taskmanager/Task.run` | 75.68% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.processMailsWhenDefaultActionUnavailable` | 72.57% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.processMail` | 72.57% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/TaskMailboxImpl.take` | 72.54% |
| `jdk/internal/misc/Unsafe.park` | 72.52% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.await` | 72.48% |
| `java/util/concurrent/locks/LockSupport.parkNanos` | 72.48% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$959/1982662702.run` | 38.27% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$947/1360819831.run` | 37.41% |
| `/cachekit-rocksdb/tmp/tm_192.168.80.5:46765-f733fe/tmp/rocksdb-lib-f69a58d3f57159dcd85a41e38d32ad19/librocksdbjni-linux-aarch64.so` | 20.80% |
| `/usr/lib64/libstdc++.so.6.0.28` | 20.56% |
| `[Rank[13] -> Sin tid=2072]` | 20.56% |
| `[Rank[13] -> Sink: nexmark_q9[14] (2/16)#0 tid=2088]` | 20.05% |
| `[Rank[13] -> Sink: nexmark_q9[14] (1/16)#0 tid=2029]` | 20.01% |
| `[Rank[13] -> Sink: nexmark_q9[14] (13/16)#0 tid=2006]` | 19.72% |
| `[Rank[13] -> Sink: nexmark_q9[14] (3/16)#0 tid=2002]` | 19.65% |
| `[GC_active]` | 3.74% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.processInput` | 3.10% |
| `org/apache/flink/streaming/runtime/io/StreamOneInputProcessor.processInput` | 3.05% |

### ready-d2 prefetch-worker scope

Share of all samples: 0.86%.

| Frame | Share within scope |
| --- | ---: |
| `/usr/lib64/libc.so.6` | 99.23% |
| `Unsafe_Park` | 98.28% |
| `Parker::park` | 98.27% |
| `pthread_cond_timedwait` | 97.12% |
| `java/util/concurrent/ThreadPoolExecutor.runWorker` | 95.30% |
| `java/util/concurrent/ThreadPoolExecutor$Worker.run` | 95.30% |
| `java/lang/Thread.run` | 95.30% |
| `java/util/concurrent/ArrayBlockingQueue.poll` | 94.31% |
| `java/util/concurrent/ThreadPoolExecutor.getTask` | 94.31% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.awaitNanos` | 94.27% |
| `jdk/internal/misc/Unsafe.park` | 94.27% |
| `java/util/concurrent/locks/LockSupport.parkNanos` | 94.26% |
| `[cachekit-bp-prefetch tid=1925]` | 50.44% |
| `[cachekit-bp-prefetch tid=1908]` | 49.56% |
| `[GC_active]` | 4.70% |
| `pthread_cond_wait` | 2.06% |
| `os::PlatformEvent::park` | 2.06% |
| `SafepointMechanism::block_if_requested_slow` | 1.29% |
| `SafepointSynchronize::block` | 1.29% |
| `Monitor::lock_without_safepoint_check` | 1.29% |
| `org/apache/flink/contrib/streaming/state/cachekit/PrefetchExecutor$CompletionTask.run` | 0.92% |
| `G1CollectedHeap::allocate_new_tlab` | 0.77% |
| `G1CollectedHeap::attempt_allocation_slow` | 0.77% |
| `MemAllocator::allocate_inside_tlab_slow` | 0.77% |
| `MemAllocator::allocate` | 0.77% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$1TrackedPrefetchTask.run` | 0.72% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunkIntoStaging` | 0.64% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.lambda$buildPreparedMultiGetTask$5` | 0.64% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunksIntoStaging` | 0.64% |
| `OptoRuntime::new_instance_C` | 0.49% |

### Largest candidate-minus-control inclusive deltas

| Frame | Control | ready-d2 | Delta |
| --- | ---: | ---: | ---: |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$957/770569290.run` | 2.33% | 0.00% | -2.33 pp |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$959/1982662702.run` | 0.00% | 2.30% | +2.30 pp |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$947/1360819831.run` | 0.00% | 2.27% | +2.27 pp |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$964/865178002.run` | 2.21% | 0.00% | -2.21 pp |
| `/cachekit-rocksdb/tmp/tm_192.168.64.6:42419-2c00dd/tmp/rocksdb-lib-fab6d5fcb6fe188038a447bdaf144f09/librocksdbjni-linux-aarch64.so` | 1.66% | 0.00% | -1.66 pp |
| `/cachekit-rocksdb/tmp/tm_192.168.80.5:46765-f733fe/tmp/rocksdb-lib-f69a58d3f57159dcd85a41e38d32ad19/librocksdbjni-linux-aarch64.so` | 0.00% | 1.59% | +1.59 pp |
| `/cachekit-rocksdb/tmp/tm_192.168.80.6:42243-d3cae2/tmp/rocksdb-lib-e82fe43b212e584a5ab6d61a22c10b8b/librocksdbjni-linux-aarch64.so` | 0.00% | 1.58% | +1.58 pp |
| `/cachekit-rocksdb/tmp/tm_192.168.64.5:40885-76e35c/tmp/rocksdb-lib-d4dcddf44be38baf212af6c8a4a1a2b8/librocksdbjni-linux-aarch64.so` | 1.57% | 0.00% | -1.57 pp |
| `jdk/internal/misc/Unsafe.park` | 47.68% | 48.88% | +1.20 pp |
| `[GC_active]` | 4.93% | 3.89% | -1.04 pp |
| `java/lang/Thread.run` | 48.82% | 49.81% | +0.99 pp |
| `java/util/concurrent/locks/LockSupport.parkNanos` | 16.75% | 17.73% | +0.98 pp |
| `pthread_cond_timedwait` | 39.43% | 40.39% | +0.96 pp |
| `java/util/concurrent/ThreadPoolExecutor$Worker.run` | 36.75% | 37.69% | +0.94 pp |
| `java/util/concurrent/ThreadPoolExecutor.runWorker` | 36.75% | 37.69% | +0.94 pp |
| `java/util/concurrent/ThreadPoolExecutor.getTask` | 26.53% | 27.40% | +0.87 pp |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.awaitNanos` | 13.54% | 14.40% | +0.86 pp |
| `pthread_cond_wait` | 37.21% | 36.37% | -0.83 pp |
| `java/util/concurrent/ScheduledThreadPoolExecutor$DelayedWorkQueue.poll` | 0.00% | 0.81% | +0.81 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$749/786147077.runDefaultAction` | 0.76% | 0.00% | -0.76 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$751/1125403750.runDefaultAction` | 0.74% | 0.00% | -0.74 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/1429291732.runDefaultAction` | 0.00% | 0.70% | +0.70 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/503107073.runDefaultAction` | 0.00% | 0.69% | +0.69 pp |
| `Unsafe_Park` | 50.69% | 51.28% | +0.58 pp |
| `Parker::park` | 50.69% | 51.27% | +0.58 pp |
| `[java tid=563]` | 0.47% | 0.00% | -0.47 pp |
| `[prometheus-http-1-4 tid=1503]` | 0.47% | 0.00% | -0.47 pp |
| `[Reference Handler tid=651]` | 0.47% | 0.00% | -0.47 pp |
| `[prometheus-http-1-3 tid=1497]` | 0.47% | 0.00% | -0.47 pp |
| `[Rank[13] -> Sin tid=2051]` | 0.47% | 0.00% | -0.47 pp |
| `[Service Thread tid=670]` | 0.47% | 0.00% | -0.47 pp |
| `[prometheus-http-1-2 tid=1481]` | 0.47% | 0.00% | -0.47 pp |
| `[flink-akka.actor.default-dispatcher-4 tid=1204]` | 0.47% | 0.00% | -0.47 pp |
| `[Flink-MetricRegistry-thread-1 tid=1334]` | 0.47% | 0.00% | -0.47 pp |
| `[Finalizer tid=652]` | 0.47% | 0.00% | -0.47 pp |
| `[Common-Cleaner tid=708]` | 0.47% | 0.00% | -0.47 pp |
| `[Log4j2-TF-3-Scheduled-1 tid=728]` | 0.47% | 0.00% | -0.47 pp |
| `[Signal Dispatcher tid=669]` | 0.47% | 0.00% | -0.47 pp |
| `[flink-akka.actor.default-dispatcher-17 tid=1378]` | 0.47% | 0.00% | -0.47 pp |
| `[taskmanager_0-main-scheduler-thread-1 tid=1384]` | 0.47% | 0.00% | -0.47 pp |

## alloc

### control top inclusive frames

| Frame | Share |
| --- | ---: |
| `java/lang/Thread.run` | 99.99% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.runMailboxLoop` | 99.27% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.runMailboxLoop` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.restoreAndInvoke` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.doRun` | 99.27% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.invoke` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.runWithSystemExitMonitoring` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.run` | 99.27% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.processInput` | 99.20% |
| `org/apache/flink/streaming/runtime/io/StreamOneInputProcessor.processInput` | 99.18% |
| `org/apache/flink/streaming/runtime/tasks/CopyingChainingOutput.collect` | 60.36% |
| `org/apache/flink/streaming/runtime/tasks/CopyingChainingOutput.pushToOperator` | 60.36% |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.emitNext` | 54.70% |
| `org/apache/flink/table/runtime/typeutils/RowDataSerializer.copy` | 52.18% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$964/865178002.run` | 50.17% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$751/1125403750.runDefaultAction` | 50.14% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$957/770569290.run` | 49.10% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$749/786147077.runDefaultAction` | 49.07% |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.processElement` | 47.96% |
| `org/apache/flink/table/runtime/typeutils/RowDataSerializer.copyRowData` | 45.50% |
| `org/apache/flink/streaming/runtime/io/StreamMultipleInputProcessor.processInput` | 44.83% |
| `com/github/nexmark/flink/source/NexmarkSourceReader.pollNext` | 44.48% |
| `org/apache/flink/streaming/runtime/io/StreamTaskSourceInput.emitNext` | 44.48% |
| `org/apache/flink/streaming/api/operators/SourceOperator.emitNext` | 44.48% |
| `org/apache/flink/table/data/binary/BinaryStringData.copy` | 44.07% |

### control rank scope

Share of all samples: 9.92%.

| Frame | Share within scope |
| --- | ---: |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.runMailboxLoop` | 100.00% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.runMailboxLoop` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.restoreAndInvoke` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.doRun` | 100.00% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.invoke` | 100.00% |
| `java/lang/Thread.run` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.runWithSystemExitMonitoring` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.run` | 100.00% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.processInput` | 99.63% |
| `org/apache/flink/streaming/runtime/io/StreamOneInputProcessor.processInput` | 99.50% |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.emitNext` | 99.50% |
| `byte[]_[i]` | 57.89% |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.processElement` | 56.26% |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.dispatchRecords` | 56.25% |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.flushBatch` | 56.25% |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.dispatchPrefix` | 56.25% |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.emitRecord` | 55.78% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$957/770569290.run` | 50.11% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$749/786147077.runDefaultAction` | 49.94% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$964/865178002.run` | 49.89% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$751/1125403750.runDefaultAction` | 49.69% |
| `org/apache/flink/table/data/binary/BinaryRowData.copy` | 48.71% |
| `org/apache/flink/streaming/runtime/tasks/OneInputStreamTask$StreamTaskNetworkOutput.emitRecord` | 46.21% |
| `org/apache/flink/runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.getNextRecord` | 42.87% |
| `org/apache/flink/runtime/plugable/NonReusingDeserializationDelegate.read` | 42.84% |
| `org/apache/flink/runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.readNextRecord` | 42.84% |
| `org/apache/flink/streaming/runtime/streamrecord/StreamElementSerializer.deserialize` | 42.84% |
| `org/apache/flink/runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.readNonSpanningRecord` | 42.10% |
| `org/apache/flink/runtime/io/network/api/serialization/NonSpanningWrapper.readInto` | 42.10% |
| `org/apache/flink/table/data/binary/BinaryRowData.copyInternal` | 41.38% |

### control prefetch-worker scope

Share of all samples: 0.01%.

| Frame | Share within scope |
| --- | ---: |
| `java/util/concurrent/ThreadPoolExecutor.runWorker` | 100.00% |
| `java/util/concurrent/ThreadPoolExecutor$Worker.run` | 100.00% |
| `java/lang/Thread.run` | 100.00% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunkIntoStaging` | 93.75% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$1TrackedPrefetchTask.run` | 93.75% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.lambda$buildPreparedMultiGetTask$5` | 93.75% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunksIntoStaging` | 93.75% |
| `[cachekit-bp-prefetch tid=1905]` | 56.25% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$$Lambda$981/888594557.run` | 50.00% |
| `java/util/ArrayList.<init>` | 43.75% |
| `java.lang.Object[]_[i]` | 43.75% |
| `[cachekit-bp-prefetch tid=1908]` | 43.75% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$$Lambda$1001/126867811.run` | 43.75% |
| `java.util.ArrayList_[i]` | 31.25% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.stagePreparedValue` | 18.75% |
| `java/util/concurrent/ConcurrentHashMap.put` | 12.50% |
| `java.util.concurrent.ConcurrentHashMap$Node_[i]` | 12.50% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.publishStagedValue` | 12.50% |
| `java/util/concurrent/ConcurrentHashMap.putVal` | 12.50% |
| `java/util/concurrent/ArrayBlockingQueue.poll` | 6.25% |
| `java/util/concurrent/ThreadPoolExecutor.getTask` | 6.25% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.awaitNanos` | 6.25% |
| `java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.addConditionWaiter` | 6.25% |
| `java.util.concurrent.locks.AbstractQueuedSynchronizer$Node_[i]` | 6.25% |
| `org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalValueState$SerializedStagedValue_[i]` | 6.25% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$StagedValue.serialized` | 6.25% |

### ready-d2 top inclusive frames

| Frame | Share |
| --- | ---: |
| `java/lang/Thread.run` | 99.99% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.runMailboxLoop` | 99.27% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.runMailboxLoop` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.restoreAndInvoke` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.doRun` | 99.27% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.invoke` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.runWithSystemExitMonitoring` | 99.27% |
| `org/apache/flink/runtime/taskmanager/Task.run` | 99.27% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.processInput` | 99.18% |
| `org/apache/flink/streaming/runtime/io/StreamOneInputProcessor.processInput` | 99.16% |
| `org/apache/flink/streaming/runtime/tasks/CopyingChainingOutput.collect` | 60.27% |
| `org/apache/flink/streaming/runtime/tasks/CopyingChainingOutput.pushToOperator` | 60.27% |
| `org/apache/flink/table/runtime/typeutils/RowDataSerializer.copy` | 52.02% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$947/1360819831.run` | 50.62% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/1429291732.runDefaultAction` | 50.57% |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.emitNext` | 49.56% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$959/1982662702.run` | 48.65% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/503107073.runDefaultAction` | 48.61% |
| `org/apache/flink/table/runtime/typeutils/RowDataSerializer.copyRowData` | 45.45% |
| `com/github/nexmark/flink/source/NexmarkSourceReader.pollNext` | 44.51% |
| `org/apache/flink/streaming/runtime/io/StreamTaskSourceInput.emitNext` | 44.51% |
| `org/apache/flink/streaming/api/operators/SourceOperator.emitNext` | 44.51% |
| `org/apache/flink/streaming/runtime/io/StreamMultipleInputProcessor.processInput` | 44.25% |
| `org/apache/flink/table/data/binary/BinaryStringData.copy` | 43.97% |
| `org/apache/flink/table/runtime/typeutils/StringDataSerializer.copy` | 43.97% |

### ready-d2 rank scope

Share of all samples: 10.46%.

| Frame | Share within scope |
| --- | ---: |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.runMailboxLoop` | 100.00% |
| `org/apache/flink/streaming/runtime/tasks/mailbox/MailboxProcessor.runMailboxLoop` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.restoreAndInvoke` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.doRun` | 100.00% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.invoke` | 100.00% |
| `java/lang/Thread.run` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.runWithSystemExitMonitoring` | 100.00% |
| `org/apache/flink/runtime/taskmanager/Task.run` | 100.00% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask.processInput` | 99.49% |
| `org/apache/flink/streaming/runtime/io/StreamOneInputProcessor.processInput` | 99.31% |
| `byte[]_[i]` | 54.31% |
| `org/apache/flink/table/data/binary/BinaryRowData.copy` | 51.19% |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.emitNext` | 50.73% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$947/1360819831.run` | 50.40% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/1429291732.runDefaultAction` | 50.12% |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$959/1982662702.run` | 49.60% |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/503107073.runDefaultAction` | 49.37% |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.dispatchReadyHead` | 49.19% |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.drainReadyBatches` | 48.67% |
| `org/apache/flink/table/data/binary/BinaryRowData.copyInternal` | 43.52% |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.dispatchRecords` | 43.33% |
| `org/apache/flink/streaming/runtime/tasks/OneInputStreamTask$StreamTaskNetworkOutput.emitRecord` | 43.31% |
| `org/apache/flink/runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.getNextRecord` | 39.46% |
| `org/apache/flink/runtime/plugable/NonReusingDeserializationDelegate.read` | 39.44% |
| `org/apache/flink/runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.readNextRecord` | 39.44% |
| `org/apache/flink/streaming/runtime/streamrecord/StreamElementSerializer.deserialize` | 39.44% |
| `org/apache/flink/runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.readNonSpanningRecord` | 38.60% |
| `org/apache/flink/runtime/io/network/api/serialization/NonSpanningWrapper.readInto` | 38.60% |
| `org/apache/flink/table/runtime/typeutils/BinaryRowDataSerializer.deserialize` | 38.06% |
| `org/apache/flink/table/runtime/typeutils/RowDataSerializer.deserialize` | 38.06% |

### ready-d2 prefetch-worker scope

Share of all samples: 0.04%.

| Frame | Share within scope |
| --- | ---: |
| `java/util/concurrent/ThreadPoolExecutor.runWorker` | 100.00% |
| `java/util/concurrent/ThreadPoolExecutor$Worker.run` | 100.00% |
| `java/lang/Thread.run` | 100.00% |
| `org/apache/flink/contrib/streaming/state/cachekit/PrefetchExecutor$CompletionTask.run` | 96.23% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunkIntoStaging` | 94.34% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$1TrackedPrefetchTask.run` | 94.34% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.lambda$buildPreparedMultiGetTask$5` | 94.34% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.fetchPreparedChunksIntoStaging` | 94.34% |
| `[cachekit-bp-prefetch tid=1908]` | 54.72% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$$Lambda$978/1416858615.run` | 49.06% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$$Lambda$999/351118875.run` | 45.28% |
| `[cachekit-bp-prefetch tid=1925]` | 45.28% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.stagePreparedValue` | 43.40% |
| `org/apache/flink/contrib/streaming/state/RocksDBValueState.getSerializedValuesByRocksDBKeys` | 35.85% |
| `org/rocksdb/RocksDB.multiGetAsList` | 33.96% |
| `org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalValueState$SerializedStagedValue_[i]` | 32.08% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState$StagedValue.serialized` | 32.08% |
| `java/util/concurrent/ConcurrentHashMap.put` | 11.32% |
| `java.util.concurrent.ConcurrentHashMap$Node_[i]` | 11.32% |
| `org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalValueState.publishStagedValue` | 11.32% |
| `java/util/concurrent/ConcurrentHashMap.putVal` | 11.32% |
| `java/util/ArrayList.<init>` | 11.32% |
| `java.lang.Object[]_[i]` | 11.32% |
| `long[]_[i]` | 9.43% |
| `byte[][]_[i]` | 9.43% |
| `int[]_[i]` | 9.43% |
| `java.util.Arrays$ArrayList_[i]` | 5.66% |
| `java/util/Arrays.asList` | 5.66% |
| `java.util.ArrayList_[i]` | 3.77% |
| `java/util/concurrent/ArrayBlockingQueue.poll` | 3.77% |

### Largest candidate-minus-control inclusive deltas

| Frame | Control | ready-d2 | Delta |
| --- | ---: | ---: | ---: |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$947/1360819831.run` | 0.00% | 50.62% | +50.62 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/1429291732.runDefaultAction` | 0.00% | 50.57% | +50.57 pp |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$964/865178002.run` | 50.17% | 0.00% | -50.17 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$751/1125403750.runDefaultAction` | 50.14% | 0.00% | -50.14 pp |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$957/770569290.run` | 49.10% | 0.00% | -49.10 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$749/786147077.runDefaultAction` | 49.07% | 0.00% | -49.07 pp |
| `org/apache/flink/runtime/taskmanager/Task$$Lambda$959/1982662702.run` | 0.00% | 48.65% | +48.65 pp |
| `org/apache/flink/streaming/runtime/tasks/StreamTask$$Lambda$750/503107073.runDefaultAction` | 0.00% | 48.61% | +48.61 pp |
| `org/apache/flink/streaming/runtime/io/StreamTwoInputProcessorFactory$$Lambda$893/1192514184.accept` | 0.00% | 19.09% | +19.09 pp |
| `org/apache/flink/streaming/runtime/io/StreamTwoInputProcessorFactory$$Lambda$908/205098231.accept` | 18.84% | 0.00% | -18.84 pp |
| `org/apache/flink/streaming/runtime/io/StreamTwoInputProcessorFactory$$Lambda$895/1005173548.accept` | 18.48% | 0.00% | -18.48 pp |
| `org/apache/flink/streaming/runtime/io/StreamTwoInputProcessorFactory$$Lambda$908/553145548.accept` | 0.00% | 17.05% | +17.05 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (11/16)#0 tid=1984]` | 12.13% | 0.00% | -12.13 pp |
| `[Join[10] -> Calc[11] (10/16)#0 tid=1993]` | 11.75% | 0.00% | -11.75 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (3/16)#0 tid=1979]` | 0.00% | 11.75% | +11.75 pp |
| `[Join[10] -> Calc[11] (7/16)#0 tid=1988]` | 11.47% | 0.00% | -11.47 pp |
| `[Join[10] -> Calc[11] (3/16)#0 tid=1994]` | 0.00% | 11.29% | +11.29 pp |
| `[Join[10] -> Calc[11] (13/16)#0 tid=2000]` | 0.00% | 11.24% | +11.24 pp |
| `[Join[10] -> Calc[11] (2/16)#0 tid=2013]` | 0.00% | 11.10% | +11.10 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (2/16)#0 tid=2002]` | 0.00% | 11.10% | +11.10 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (13/16)#0 tid=1990]` | 0.00% | 11.06% | +11.06 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (7/16)#0 tid=1977]` | 10.93% | 0.00% | -10.93 pp |
| `[Join[10] -> Calc[11] (1/16)#0 tid=1991]` | 10.93% | 0.00% | -10.93 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (10/16)#0 tid=1986]` | 10.78% | 0.00% | -10.78 pp |
| `[Join[10] -> Calc[11] (11/16)#0 tid=1994]` | 10.68% | 0.00% | -10.68 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (1/16)#0 tid=1979]` | 10.67% | 0.00% | -10.67 pp |
| `[Join[10] -> Calc[11] (1/16)#0 tid=2010]` | 0.00% | 10.64% | +10.64 pp |
| `[Source: datagen[1] -> (Calc[2] -> WatermarkAssigner[3] -> Calc[4], Calc[6] -> WatermarkAssigner[7] -> Calc[8]) (1/16)#0 tid=1997]` | 0.00% | 10.63% | +10.63 pp |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.dispatchPrefix` | 5.58% | 0.00% | -5.58 pp |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.flushBatch` | 5.58% | 0.07% | -5.51 pp |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.dispatchReadyHead` | 0.00% | 5.15% | +5.15 pp |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.emitNext` | 54.70% | 49.56% | -5.14 pp |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.drainReadyBatches` | 0.00% | 5.09% | +5.09 pp |
| `org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.processElement` | 47.96% | 42.99% | -4.97 pp |
| `org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput.emitRecord` | 5.54% | 1.07% | -4.46 pp |
| `org/apache/flink/table/data/RowData$$Lambda$766/153317512.getFieldOrNull` | 0.00% | 3.48% | +3.48 pp |
| `org/apache/flink/streaming/runtime/io/StreamTwoInputProcessorFactory$$Lambda$907/81758093.accept` | 0.00% | 3.44% | +3.44 pp |
| `org/apache/flink/table/data/RowData$$Lambda$767/1692484245.getFieldOrNull` | 3.41% | 0.00% | -3.41 pp |
| `org/apache/flink/table/data/RowData$$Lambda$770/534300939.getFieldOrNull` | 3.41% | 0.00% | -3.41 pp |
| `org/apache/flink/table/data/RowData$$Lambda$764/837832101.getFieldOrNull` | 0.00% | 3.35% | +3.35 pp |


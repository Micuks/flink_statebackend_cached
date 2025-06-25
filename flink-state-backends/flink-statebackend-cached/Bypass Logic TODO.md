# TODO: Fix CachingInternalMapState Bypass Logic

The current implementation of the cache bypass mechanism in `CachingInternalMapState` is a one-way switch. Once the cache hit rate drops below the configured threshold, the bypass is activated and all subsequent operations are sent directly to the delegate state. This prevents the system from ever measuring an improved hit rate, so the cache can never be re-enabled automatically.

This plan outlines the steps to fix this by using a continuous sampling strategy. Even when the cache is bypassed, a small percentage of requests will still be sent through the cache path. This allows the hit-rate monitor to observe if the cache has become useful again and re-enable it automatically.

- [x] **Introduce State for Sampling**
    - In `CachingInternalMapState.java`, add a new `transient AtomicLong` counter for sampling, e.g., `accessSampler`.
    - Add a constant to define the sampling rate, e.g., `private static final int SAMPLING_RATE = 100;` (for a 1% sample rate).
    - Initialize the counter in the constructor.

- [x] **Modify Public Methods to Allow Sampling**
    - In the primary public methods (`get`, `put`, `remove`, `contains`), update the bypass check.
    - The current logic is: `if (bypassEnabled && bypassCache) { ... }`.
    - The new logic should be:
        1. Check if bypass is active: `if (bypassEnabled && bypassCache)`.
        2. Inside the block, decide if the current request should be a sample (e.g., `accessSampler.incrementAndGet() % SAMPLING_RATE == 0`).
        3. If it's **not** a sample, proceed with the bypass as before (go to the delegate).
        4. If it **is** a sample, skip the bypass logic and allow the operation to proceed to the normal caching path.

- [x] **Update `updateCacheBypassCondition`**
    - The `updateCacheBypassCondition` method can now be simplified. It no longer needs a special mode for when bypass is active.
    - It will continuously receive hit/miss information from:
        - All requests when the cache is active.
        - Bypassed requests (always a miss).
        - Sampled requests (real hit or miss).
    - This will allow the hit rate to recover naturally. If the hit rate of the sampled traffic rises above the threshold, the `bypassCache` flag will automatically be set to `false`.

- [ ] **Update Unit Tests for Verification**
    - In `CachingInternalMapStateTest.java`, rewrite the bypass reactivation test to verify the sampling mechanism.
    - The new test should:
        1.  Simulate a workload with a low cache hit rate to trigger the bypass. Assert bypass becomes active.
        2.  Simulate a series of operations that will be mostly bypassed.
        3.  Inject "sampled" operations that result in cache hits.
        4.  Continue until a full window has been processed with a high-enough sampled hit rate.
        5.  Assert that `isBypassCacheActive()` becomes `false`, re-enabling the cache.

- [x] **Fix duplicate delegateState.remove() call**
    - Fixed the issue where remove() was calling delegateState.remove() twice - once directly and once through the eviction listener when the tombstone is flushed. 
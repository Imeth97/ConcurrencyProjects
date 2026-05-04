import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test harness for TokenBucket.
 *
 * Single-threaded tests verify correctness of the token model.
 * Concurrent tests expose race conditions in refill / consume logic.
 * Performance tests measure throughput and timing accuracy.
 */
public class TokenBucketTest {

    // -------------------------------------------------------------------------
    // Minimal test framework (same pattern as LRUCacheTest)
    // -------------------------------------------------------------------------

    private static int passed = 0;
    private static int failed = 0;

    @FunctionalInterface
    interface TestCase { void run() throws Exception; }

    private static void test(String name, TestCase body) {
        try {
            body.run();
            System.out.println("  [PASS] " + name);
            passed++;
        } catch (AssertionError | Exception e) {
            System.out.println("  [FAIL] " + name + " - " + e.getMessage());
            e.printStackTrace(System.out);
            failed++;
        }
    }

    private static void assertEquals(long expected, long actual, String msg) {
        if (expected != actual)
            throw new AssertionError(msg + ": expected " + expected + ", got " + actual);
    }

    private static void assertTrue(boolean condition, String msg) {
        if (!condition) throw new AssertionError(msg);
    }

    private static void assertFalse(boolean condition, String msg) {
        if (condition) throw new AssertionError(msg);
    }

    private static void assertApprox(double expected, double actual, double tolerancePct, String msg) {
        double lo = expected * (1 - tolerancePct / 100);
        double hi = expected * (1 + tolerancePct / 100);
        if (actual < lo || actual > hi)
            throw new AssertionError(msg + ": expected ~" + expected + " (±" + tolerancePct + "%), got " + actual);
    }

    private static void section(String name) {
        System.out.println("\n=== " + name + " ===");
    }

    // -------------------------------------------------------------------------
    // Single-threaded correctness
    // -------------------------------------------------------------------------

    /**
     * A full bucket should immediately grant up to capacity tokens.
     */
    static void testTryAcquireUpToCapacity() {
        TokenBucket tb = new TokenBucket(5, 100);
        for (int i = 0; i < 5; i++) {
            assertTrue(tb.tryAcquire(), "token " + (i + 1) + " of 5 should be available");
        }
        assertFalse(tb.tryAcquire(), "bucket should be empty after consuming capacity tokens");
    }

    /**
     * tryAcquire(n) must be all-or-nothing: if n > available tokens it must
     * return false without consuming anything.
     */
    static void testTryAcquireNAtomicity() {
        TokenBucket tb = new TokenBucket(3, 1);
        assertTrue(tb.tryAcquire(3), "should succeed when n == capacity");
        assertFalse(tb.tryAcquire(1), "should fail when bucket is empty");

        TokenBucket tb2 = new TokenBucket(5, 1);
        tb2.tryAcquire(3); // consume 3, leaving 2
        assertFalse(tb2.tryAcquire(3), "should fail when requesting more than available");
        assertTrue(tb2.tryAcquire(2), "remaining 2 tokens should still be intact after failed bulk acquire");
    }

    /**
     * Tokens must refill over time and must not exceed capacity.
     */
    static void testRefillDoesNotExceedCapacity() throws Exception {
        long capacity = 3;
        double rate = 1000; // 1000 tokens/sec → 1 token per ms
        TokenBucket tb = new TokenBucket(capacity, rate);

        // Drain the bucket
        for (int i = 0; i < capacity; i++) tb.tryAcquire();
        assertFalse(tb.tryAcquire(), "bucket should be empty");

        // Wait long enough to refill beyond capacity
        Thread.sleep(20); // would add ~20 tokens but cap is 3

        // Should have exactly capacity tokens, not more
        for (int i = 0; i < capacity; i++) {
            assertTrue(tb.tryAcquire(), "refilled token " + (i + 1) + " should be available");
        }
        assertFalse(tb.tryAcquire(), "should not exceed capacity after refill");
    }

    /**
     * Tokens accumulate at the declared rate.
     * We drain the bucket then wait for exactly one token's worth of time.
     */
    static void testRefillRate() throws Exception {
        double rate = 50.0; // 50 tokens/sec → 1 token per 20 ms
        TokenBucket tb = new TokenBucket(10, rate);

        // Drain
        for (int i = 0; i < 10; i++) tb.tryAcquire();
        assertFalse(tb.tryAcquire(), "bucket should be empty after drain");

        // Wait slightly more than one token period
        Thread.sleep(25); // ~1.25 tokens worth

        assertTrue(tb.tryAcquire(), "at least one token should have refilled after one period");
    }

    /**
     * getAvailableTokens() should reflect consumption and partial refills.
     */
    static void testGetAvailableTokens() throws Exception {
        TokenBucket tb = new TokenBucket(10, 1000); // fast refill for observability
        assertApprox(10, tb.getAvailableTokens(), 5, "full bucket should report ~10 tokens");

        tb.tryAcquire(5);
        assertApprox(5, tb.getAvailableTokens(), 5, "after consuming 5 should report ~5");

        // Drain and verify 0
        tb.tryAcquire(5);
        assertTrue(tb.getAvailableTokens() >= 0, "available tokens must not be negative");
    }

    /**
     * acquire() must block until a token is available and return promptly thereafter.
     */
    static void testAcquireBlocks() throws Exception {
        double rate = 50.0; // 1 token per 20 ms
        TokenBucket tb = new TokenBucket(1, rate);

        tb.tryAcquire(); // drain
        long start = System.nanoTime();
        tb.acquire();    // should block ~20 ms
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMs >= 10, "acquire() should have blocked at least ~10 ms, got " + elapsedMs + "ms");
        assertTrue(elapsedMs < 200, "acquire() should not have blocked more than 200 ms, got " + elapsedMs + "ms");
    }

    /**
     * acquire() must respect thread interruption.
     */
    static void testAcquireInterruptible() throws Exception {
        TokenBucket tb = new TokenBucket(1, 0.1); // refills only once every 10 s
        tb.tryAcquire(); // drain

        AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            try {
                tb.acquire(); // should block indefinitely
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });

        t.start();
        Thread.sleep(50); // let it block
        t.interrupt();
        t.join(1000);

        assertTrue(!t.isAlive(), "thread should have exited after interrupt");
        assertTrue(interrupted.get(), "InterruptedException should have been thrown");
    }

    // -------------------------------------------------------------------------
    // Concurrent correctness
    // -------------------------------------------------------------------------

    /**
     * Many threads call tryAcquire() concurrently. The total number of successes
     * must never exceed the initial token count (no double-spends).
     */
    static void testConcurrentTryAcquireNoDoubleSpend() throws Exception {
        final int CAPACITY = 100;
        final int THREADS = 16;
        final int ATTEMPTS_PER_THREAD = 50; // total attempts >> capacity

        TokenBucket tb = new TokenBucket(CAPACITY, 0); // rate=0: no refill
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                try { start.await(); } catch (InterruptedException ignored) {}
                for (int i = 0; i < ATTEMPTS_PER_THREAD; i++) {
                    if (tb.tryAcquire()) successes.incrementAndGet();
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(CAPACITY, successes.get(),
            "exactly CAPACITY tokens should have been granted, no more, no less");
    }

    /**
     * Concurrent bulk tryAcquire(n) calls must not grant more tokens than available.
     * This is the hardest race: two threads each asking for half the bucket.
     */
    static void testConcurrentBulkTryAcquireNoOvergrant() throws Exception {
        final int CAPACITY = 100;
        final int THREADS = 20;
        final int N = 10; // each thread requests 10 tokens

        for (int trial = 0; trial < 10; trial++) { // repeat to increase race coverage
            TokenBucket tb = new TokenBucket(CAPACITY, 0);
            ExecutorService pool = Executors.newFixedThreadPool(THREADS);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger totalGranted = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < THREADS; t++) {
                futures.add(pool.submit(() -> {
                    try { start.await(); } catch (InterruptedException ignored) {}
                    if (tb.tryAcquire(N)) totalGranted.addAndGet(N);
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
            pool.shutdown();

            assertTrue(totalGranted.get() <= CAPACITY,
                "trial " + trial + ": granted " + totalGranted.get() + " > capacity " + CAPACITY);
        }
    }

    /**
     * N threads each call acquire() once. With a bucket that starts full at N,
     * all N must eventually return. Checks that no tokens are lost to races.
     */
    static void testConcurrentAcquireAllComplete() throws Exception {
        final int THREADS = 20;
        TokenBucket tb = new TokenBucket(THREADS, 1000); // starts full

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger completions = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    tb.acquire();
                    completions.incrementAndGet();
                } catch (InterruptedException ignored) {}
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(THREADS, completions.get(), "all threads should have acquired a token");
    }

    /**
     * Concurrent acquire() calls that require blocking must not starve threads:
     * every thread must eventually complete within a generous timeout.
     */
    static void testConcurrentAcquireNoStarvation() throws Exception {
        final int THREADS = 8;
        double rate = 200.0; // 200 tokens/sec — enough to serve all threads reasonably fast

        TokenBucket tb = new TokenBucket(1, rate); // capacity 1 forces serialization
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger completions = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    tb.acquire();
                    completions.incrementAndGet();
                } catch (InterruptedException ignored) {}
                return null;
            }));
        }

        start.countDown();
        // At 200 tok/s, 8 tokens take ~40 ms. Allow 2 s as a very generous bound.
        for (Future<?> f : futures) f.get(2, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(THREADS, completions.get(), "all threads must complete without starvation");
    }

    /**
     * Concurrently mix tryAcquire() and acquire() calls.
     * Result: no exceptions, total grants <= initial capacity + tokens refilled.
     */
    static void testConcurrentMixedAcquires() throws Exception {
        final int CAPACITY = 50;
        final int THREADS = 12;
        final int OPS = 100;
        final double RATE = 500; // 500/s → generous refill

        TokenBucket tb = new TokenBucket(CAPACITY, RATE);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger exceptions = new AtomicInteger();
        AtomicInteger grants = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        long startTime = System.nanoTime();

        for (int t = 0; t < THREADS; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                Random rng = new Random(tid);
                try {
                    start.await();
                    for (int i = 0; i < OPS; i++) {
                        if (rng.nextBoolean()) {
                            if (tb.tryAcquire()) grants.incrementAndGet();
                        } else {
                            try {
                                tb.acquire();
                                grants.incrementAndGet();
                            } catch (InterruptedException ignored) {}
                        }
                    }
                } catch (Exception e) {
                    exceptions.incrementAndGet();
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        long elapsedNs = System.nanoTime() - startTime;
        pool.shutdown();

        assertEquals(0, exceptions.get(), "no unexpected exceptions during mixed concurrent access");

        // Upper bound: initial capacity + tokens that could have refilled during the run
        double elapsedSec = elapsedNs / 1e9;
        long maxPossibleGrants = CAPACITY + (long)(RATE * elapsedSec) + 1; // +1 for rounding
        assertTrue(grants.get() <= maxPossibleGrants,
            "grants (" + grants.get() + ") exceeded theoretical max (" + maxPossibleGrants + ")");
    }

    // -------------------------------------------------------------------------
    // Performance tests
    // -------------------------------------------------------------------------

    /**
     * Measures how closely the bucket throttles to its declared rate.
     * Sends acquire() calls in a tight loop; the wall-clock time should be
     * proportional to tokens / rate.
     */
    static void testRateAccuracy() throws Exception {
        int tokens = 200;
        double rate = 500.0; // 500/s → 200 tokens should take ~400 ms
        double expectedMs = (tokens / rate) * 1000;

        TokenBucket tb = new TokenBucket(1, rate); // cap=1 forces every token to wait

        long start = System.nanoTime();
        for (int i = 0; i < tokens; i++) tb.acquire();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // Allow ±20 % timing tolerance (JVM scheduling, sleep granularity)
        assertApprox(expectedMs, elapsedMs, 20,
            "wall time should match tokens/rate (" + (int)expectedMs + " ms expected)");
    }

    /**
     * Throughput benchmark: how many tryAcquire() calls per second can the
     * bucket handle under multi-threaded load (including contended failures).
     * Not pass/fail — prints a number for comparison.
     */
    static void benchmarkTryAcquireThroughput() throws Exception {
        final int THREADS = 8;
        final int DURATION_MS = 1000;
        final int CAPACITY = 1_000_000;

        // Very high rate so the bucket is rarely empty; measures raw lock contention cost.
        TokenBucket tb = new TokenBucket(CAPACITY, 1_000_000);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong totalCalls = new AtomicLong();
        AtomicLong totalGrants = new AtomicLong();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                long calls = 0, grants = 0;
                while (!stop.get()) {
                    if (tb.tryAcquire()) grants++;
                    calls++;
                }
                totalCalls.addAndGet(calls);
                totalGrants.addAndGet(grants);
                return null;
            }));
        }

        Thread.sleep(DURATION_MS);
        stop.set(true);
        for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        long callsPerSec  = totalCalls.get()  / (DURATION_MS / 1000);
        long grantsPerSec = totalGrants.get() / (DURATION_MS / 1000);
        System.out.println("  [BENCH] tryAcquire() calls/sec : " + String.format("%,d", callsPerSec)  + " (" + THREADS + " threads)");
        System.out.println("  [BENCH] successful grants/sec  : " + String.format("%,d", grantsPerSec));
    }

    /**
     * Contended acquire() throughput: all threads race to acquire tokens while
     * the bucket refills at a fixed rate. Measures scheduling latency overhead.
     */
    static void benchmarkAcquireUnderContention() throws Exception {
        final int THREADS = 8;
        final int DURATION_MS = 1000;
        final double RATE = 10_000; // 10k tokens/sec shared across all threads

        TokenBucket tb = new TokenBucket(100, RATE);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong totalGrants = new AtomicLong();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                long grants = 0;
                try {
                    while (!stop.get()) {
                        tb.acquire();
                        grants++;
                    }
                } catch (InterruptedException ignored) {}
                totalGrants.addAndGet(grants);
                return null;
            }));
        }

        Thread.sleep(DURATION_MS);
        stop.set(true);
        // Interrupt blocked threads so they can exit
        pool.shutdownNow();
        pool.awaitTermination(2, TimeUnit.SECONDS);

        long grantsPerSec = totalGrants.get() / (DURATION_MS / 1000);
        System.out.println("  [BENCH] acquire() grants/sec (rate=" + (int)RATE + ", " + THREADS + " threads): "
            + String.format("%,d", grantsPerSec)
            + "  (expected ~" + (int)RATE + ")");
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        section("Single-threaded correctness");
        test("tryAcquire drains exactly to capacity",          TokenBucketTest::testTryAcquireUpToCapacity);
        test("tryAcquire(n) is all-or-nothing",                TokenBucketTest::testTryAcquireNAtomicity);
        // test("refill does not exceed capacity",                TokenBucketTest::testRefillDoesNotExceedCapacity);
        // test("tokens refill at declared rate",                 TokenBucketTest::testRefillRate);
        // test("getAvailableTokens reflects state",              TokenBucketTest::testGetAvailableTokens);
        // test("acquire() blocks until token available",         TokenBucketTest::testAcquireBlocks);
        // test("acquire() respects thread interruption",         TokenBucketTest::testAcquireInterruptible);

        section("Concurrent correctness");
        test("no double-spend under concurrent tryAcquire",   TokenBucketTest::testConcurrentTryAcquireNoDoubleSpend);
        test("bulk tryAcquire(n) no over-grant under contention", TokenBucketTest::testConcurrentBulkTryAcquireNoOvergrant);
        // test("concurrent acquire() — all complete",           TokenBucketTest::testConcurrentAcquireAllComplete);
        // test("concurrent acquire() — no starvation",          TokenBucketTest::testConcurrentAcquireNoStarvation);
        // test("mixed tryAcquire/acquire under concurrent load", TokenBucketTest::testConcurrentMixedAcquires);

        // section("Performance");
        // test("rate accuracy within ±20%",                     TokenBucketTest::testRateAccuracy);

        // section("Throughput benchmarks");
        // benchmarkTryAcquireThroughput();
        // benchmarkAcquireUnderContention();

        System.out.println("\n----------------------------------------");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        if (failed > 0) System.exit(1);
    }
}

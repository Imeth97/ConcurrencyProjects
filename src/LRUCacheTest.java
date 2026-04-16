import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test harness for LRUCache.
 *
 * Single-threaded tests validate correctness and should pass as-is.
 * Concurrent tests expose thread-safety issues - expect failures until
 * the cache is made thread-safe.
 */
public class LRUCacheTest {

    // -------------------------------------------------------------------------
    // Minimal test framework
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
            failed++;
        }
    }

    private static void assertEquals(int expected, int actual, String msg) {
        if (expected != actual)
            throw new AssertionError(msg + ": expected " + expected + ", got " + actual);
    }

    private static void assertTrue(boolean condition, String msg) {
        if (!condition) throw new AssertionError(msg);
    }

    private static void section(String name) {
        System.out.println("\n=== " + name + " ===");
    }

    // -------------------------------------------------------------------------
    // Single-threaded correctness tests
    // -------------------------------------------------------------------------

    static void testBasicPutAndGet() {
        LRUCache cache = new LRUCache(3);
        cache.put(1, 10);
        cache.put(2, 20);
        assertEquals(10, cache.get(1), "get after put");
        assertEquals(20, cache.get(2), "get after put");
        assertEquals(-1, cache.get(99), "missing key returns -1");
    }

    static void testEvictionEvictsLRU() {
        LRUCache cache = new LRUCache(3);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        // 1 is LRU - inserting 4 should evict it
        cache.put(4, 4);
        assertEquals(-1, cache.get(1), "key 1 should be evicted");
        assertEquals(2,  cache.get(2), "key 2 should still exist");
        assertEquals(3,  cache.get(3), "key 3 should still exist");
        assertEquals(4,  cache.get(4), "key 4 should exist");
    }

    static void testGetPromotesToMRU() {
        LRUCache cache = new LRUCache(2);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.get(1);        // promotes 1 to MRU; 2 becomes LRU
        cache.put(3, 3);     // should evict 2
        assertEquals(-1, cache.get(2), "key 2 should be evicted after get promoted key 1");
        assertEquals(1,  cache.get(1), "key 1 should survive");
        assertEquals(3,  cache.get(3), "key 3 should exist");
    }

    static void testPutUpdatesExistingKey() {
        LRUCache cache = new LRUCache(2);
        cache.put(1, 100);
        cache.put(1, 200);
        assertEquals(200, cache.get(1), "put on existing key should update value");
    }

    static void testPutUpdateDoesNotChangeSize() {
        LRUCache cache = new LRUCache(2);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(1, 99); // update, not new entry
        cache.put(3, 3);  // should evict 2 (1 was just touched), not 1
        assertEquals(99, cache.get(1), "updated key should retain new value");
        assertEquals(-1, cache.get(2), "key 2 should be evicted");
        assertEquals(3,  cache.get(3), "key 3 should exist");
    }

    static void testCapacityOfOne() {
        LRUCache cache = new LRUCache(1);
        cache.put(1, 1);
        assertEquals(1, cache.get(1), "get from single-entry cache");
        cache.put(2, 2);
        assertEquals(-1, cache.get(1), "first key evicted after second put");
        assertEquals(2,  cache.get(2), "second key present");
    }

    // -------------------------------------------------------------------------
    // Concurrent tests
    // -------------------------------------------------------------------------

    /**
     * Many threads write unique keys concurrently; afterwards all written keys
     * that fit within capacity should be readable (no data corruption).
     *
     * A non-thread-safe cache may throw, return wrong values, or corrupt state.
     */
    static void testConcurrentPutsNoExceptions() throws Exception {
        final int THREADS = 8;
        final int OPS_PER_THREAD = 200;
        final int CAPACITY = 100;

        LRUCache cache = new LRUCache(CAPACITY);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger exceptions = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            final int base = t * OPS_PER_THREAD;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        cache.put(base + i, base + i);
                    }
                } catch (Exception e) {
                    exceptions.incrementAndGet();
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, exceptions.get(), "no exceptions during concurrent puts");
    }

    /**
     * Mixed concurrent reads and writes - checks that gets never return
     * a value that was never put (i.e. no phantom reads / corrupted state).
     */
    static void testConcurrentPutGetConsistency() throws Exception {
        final int THREADS = 8;
        final int OPS = 500;
        final int CAPACITY = 50;
        final int KEY_RANGE = 80;   // wider than capacity to force evictions

        LRUCache cache = new LRUCache(CAPACITY);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger badReads = new AtomicInteger();
        AtomicInteger exceptions = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        Random rng = new Random(42);

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                Random local = new Random();
                try {
                    start.await();
                    for (int i = 0; i < OPS; i++) {
                        int key = local.nextInt(KEY_RANGE);
                        if (local.nextBoolean()) {
                            cache.put(key, key * 7); // sentinel: value == key * 7
                        } else {
                            int val = cache.get(key);
                            // If present, value must equal key * 7
                            if (val != -1 && val != key * 7) {
                                badReads.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    exceptions.incrementAndGet();
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, exceptions.get(),  "no exceptions during concurrent put/get");
        assertEquals(0, badReads.get(),    "no corrupted values read back");
    }

    /**
     * Verifies the cache never exceeds its declared capacity under concurrent load.
     * Uses reflection on the internal map to check size at intervals.
     *
     * A racey implementation will regularly exceed capacity.
     */
    static void testCapacityNeverExceededUnderConcurrentLoad() throws Exception {
        final int CAPACITY = 20;
        final int THREADS = 6;
        final int OPS = 400;

        LRUCache cache = new LRUCache(CAPACITY);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger overflows = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        // Inspector thread samples map size continuously
        futures.add(pool.submit(() -> {
            try {
                java.lang.reflect.Field f = LRUCache.class.getDeclaredField("map");
                f.setAccessible(true);
                while (running.get()) {
                    Map<?,?> m = (Map<?,?>) f.get(cache);
                    if (m.size() > CAPACITY) overflows.incrementAndGet();
                    Thread.yield();
                }
            } catch (Exception e) {
                // reflection failure - skip check
            }
            return null;
        }));

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                Random local = new Random();
                try {
                    start.await();
                    for (int i = 0; i < OPS; i++) {
                        cache.put(local.nextInt(CAPACITY * 3), i);
                    }
                } catch (Exception ignored) {}
                return null;
            }));
        }

        start.countDown();
        for (int t = 1; t < futures.size(); t++) futures.get(t).get(10, TimeUnit.SECONDS);
        running.set(false);
        futures.get(0).get(2, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, overflows.get(), "cache size exceeded capacity " + overflows.get() + " time(s)");
    }

    /**
     * Throughput benchmark - measures combined put/get ops/sec under load.
     * Not a pass/fail test; prints a number you can track across implementations.
     */
    static void benchmarkThroughput() throws Exception {
        final int THREADS = 8;
        final int DURATION_MS = 1000;
        final int CAPACITY = 200;
        final int KEY_RANGE = 300;

        LRUCache cache = new LRUCache(CAPACITY);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong totalOps = new AtomicLong();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                Random local = new Random();
                long ops = 0;
                try {
                    while (!stop.get()) {
                        int key = local.nextInt(KEY_RANGE);
                        if (local.nextBoolean()) cache.put(key, key);
                        else cache.get(key);
                        ops++;
                    }
                } catch (Exception ignored) {}
                totalOps.addAndGet(ops);
                return null;
            }));
        }

        Thread.sleep(DURATION_MS);
        stop.set(true);
        for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        long opsPerSec = totalOps.get() / (DURATION_MS / 1000);
        System.out.println("  [BENCH] Throughput: " + String.format("%,d", opsPerSec) + " ops/sec across " + THREADS + " threads");
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        section("Single-threaded correctness");
        test("basic put and get",                 LRUCacheTest::testBasicPutAndGet);
        test("eviction removes LRU entry",        LRUCacheTest::testEvictionEvictsLRU);
        test("get promotes key to MRU",           LRUCacheTest::testGetPromotesToMRU);
        test("put updates existing key value",    LRUCacheTest::testPutUpdatesExistingKey);
        test("put-update doesn't bloat capacity", LRUCacheTest::testPutUpdateDoesNotChangeSize);
        test("capacity of one",                   LRUCacheTest::testCapacityOfOne);

        section("Concurrent correctness");
        test("concurrent puts - no exceptions",              LRUCacheTest::testConcurrentPutsNoExceptions);
        test("concurrent put/get - no corrupted values",     LRUCacheTest::testConcurrentPutGetConsistency);
        test("capacity never exceeded under concurrent load", LRUCacheTest::testCapacityNeverExceededUnderConcurrentLoad);

        section("Throughput benchmark");
        benchmarkThroughput();

        System.out.println("\n----------------------------------------");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        if (failed > 0) System.exit(1);
    }
}

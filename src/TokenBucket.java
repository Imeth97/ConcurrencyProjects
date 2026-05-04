import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Token bucket rate limiter.
 *
 * Tokens refill at a fixed rate (tokensPerSecond) up to a maximum burst capacity.
 * tryAcquire() returns true if a token was available and consumed, false otherwise.
 * acquire() blocks until a token is available.
 */
public class TokenBucket {

    // TODO: implement fields

    private long tokens;

    private final long capacity;

    private final ReentrantLock lock;

    /**
     * @param capacity      maximum tokens the bucket can hold (burst size)
     * @param tokensPerSecond  refill rate
     */
    public TokenBucket(long capacity, double tokensPerSecond) {
        this.capacity = capacity;
        this.tokens = capacity;
        this.lock = new ReentrantLock();
        initTokenRefiller(tokensPerSecond);

    }

    private void initTokenRefiller(double tokensPerSecond) {
        new Thread(() -> {
            try {
                while (true) {
                    lock.lock();
                    if (tokens < capacity) {
                        System.out.printf("Refilling tokens: %f \n", tokensPerSecond);
                        tokens = Math.min( tokens + Math.round(tokensPerSecond), capacity);
                    } else {
                        System.out.println("Tokens full");
                    }
                    lock.unlock();
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    /**
     * Attempt to consume one token without blocking.
     * @return true if a token was consumed, false if the bucket is empty
     */
    public boolean tryAcquire() {
        lock.lock();
        boolean canAcquire = false;
        if (tokens > 0) {
            canAcquire = true;
            tokens--;
        }
        lock.unlock();
        return canAcquire;
    }

    /**
     * Attempt to consume n tokens without blocking.
     * @return true if all n tokens were consumed atomically, false otherwise
     */
    public boolean tryAcquire(int n) {
        lock.lock();
        boolean canAcquire = false;
        if (tokens >= n) {
            canAcquire = true;
            tokens -= n;
        }
        lock.unlock();
        return canAcquire;
    }

    /**
     * Block until a token is available, then consume it.
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void acquire() throws InterruptedException {
        // TODO: implement
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Block until n tokens are available, then consume them atomically.
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void acquire(int n) throws InterruptedException {
        // TODO: implement
        throw new UnsupportedOperationException("not implemented");
    }

    /** Current token count (snapshot — may change immediately after return). */
    public double getAvailableTokens() {
        // TODO: implement
        throw new UnsupportedOperationException("not implemented");
    }
}

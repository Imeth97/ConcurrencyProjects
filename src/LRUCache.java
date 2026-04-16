import java.util.LinkedHashMap;
import java.util.concurrent.locks.ReentrantLock;

// Thread Safe
public class LRUCache {

    private LinkedHashMap<Integer, Integer> map;
    private final int cap;
    private ReentrantLock lock = new ReentrantLock();

    public LRUCache(int capacity) {
        this.map = new LinkedHashMap<>();
        this.cap = capacity;
    }

    public int get(int key) {
        lock.lock();
        try {
            Integer val = this.map.get(key);
            if (val == null) {
                return -1;
            }
            this.put(key, val, false);
            return val;
        } finally {
            lock.unlock();
        }
    }

    private void put(int key, int value, boolean shouldUnlock) {
        if (shouldUnlock) {
            lock.lock();
        }
        try {
            this.map.remove(key);
            this.map.put(key, value);
            if (this.map.size() > cap) {
                int oldestEntry = this.map.keySet().iterator().next();
                this.map.remove(oldestEntry);
            }
        } finally {
            if (shouldUnlock) {
                lock.unlock();
            }
        }
    }

    public void put(int key, int value) {
        this.put(key, value, true);
    }
}
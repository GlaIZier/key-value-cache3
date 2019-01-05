package ru.glaizier.key.value.cache3.cache.strategy;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * @author GlaIZier
 */
@ThreadSafe
public class ConcurrentLruStrategy1<K> implements Strategy<K> {

    @GuardedBy("useLock")
    private final Queue<K> q = new ConcurrentLinkedQueue<>();

    private final ConcurrentMap<K, Boolean> keys = new ConcurrentHashMap<K, Boolean>();

    @Override
    // smart locking or help with operations
    public Optional<K> evict() {
        while (true) {
            K evicted = q.peek();
            // lock
            return Optional.ofNullable(q.poll());

        }

    }

    /**
     * O(1)
     */
    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        q.add(key);
        Boolean prev = keys.put(key, true);

        return prev != null;
    }

    /**
     * O(n) * number of keys in the queue
     */
    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        Boolean exists = keys.remove(key);
        if (exists != null)
            while (true)
                if (!q.remove(key))
                    break;
        return exists != null;
    }
}

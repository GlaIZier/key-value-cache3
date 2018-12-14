package ru.glaizier.key.value.cache3.cache.strategy;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * @author GlaIZier
 */
@ThreadSafe
// Todo come up with an idea how to use Navigable map to speed it up
public class ConcurrentLruStrategy<K> implements Strategy<K> {

    @GuardedBy("useLock")
    private final Queue<K> q = new ConcurrentLinkedQueue<>();

    private final Object useLock = new Object();

    @Override
    public Optional<K> evict() {
        return Optional.ofNullable(q.poll());
    }

    /**
     * O(n)
     */
    @Override
    // Todo try to introduce Atomic reference?
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        boolean contained;
        synchronized (useLock) {
            contained = q.remove(key);
            q.add(key);
        }
        return contained;
    }

    /**
     * O(n)
     */
    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return q.remove(key);
    }
}

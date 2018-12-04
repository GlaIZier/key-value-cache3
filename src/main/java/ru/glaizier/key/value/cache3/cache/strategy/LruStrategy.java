package ru.glaizier.key.value.cache3.cache.strategy;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * @author GlaIZier
 */
@ThreadSafe
public class LruStrategy<K> implements Strategy<K> {

    /**
     * We need to be able to get by key, replace elements and get first in queue in O(1).
     * This can be achieved by Java SE means. This set starts iteration from the last added element
     */
    @GuardedBy("lock")
    private final Set<K> queue = new LinkedHashSet<>();

    private final Object lock = new Object();

    @Override
    public Optional<K> evict() {
        // Find first element for eviction and remove it if it was found
        synchronized (lock) {
            return queue.stream()
                .findFirst()
                .map(evictedKey -> {
                    queue.remove(evictedKey);
                    return evictedKey;
                });
        }
    }

    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        synchronized (lock) {
            boolean contained = queue.remove(key);
            queue.add(key);
            return contained;
        }
    }

    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        synchronized (lock) {
            return queue.remove(key);
        }
    }
}

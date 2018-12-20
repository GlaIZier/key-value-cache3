package ru.glaizier.key.value.cache3.cache.strategy;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * @author GlaIZier
 */
@ThreadSafe
public class SynchronizedStrategy<K> implements Strategy<K> {

    @GuardedBy("lock")
    private final Strategy<K> strategy;

    private final Object lock = new Object();

    public SynchronizedStrategy(Strategy<K> queue) {
        this.strategy = queue;
    }

    @Override
    public Optional<K> evict() {
        // Find first element for eviction and remove it if it was found
        synchronized (lock) {
            return strategy.evict();
        }
    }

    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        synchronized (lock) {
            return strategy.use(key);
        }
    }

    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        synchronized (lock) {
            return strategy.remove(key);
        }
    }
}

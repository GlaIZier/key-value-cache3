package ru.glaizier.key.value.cache3.cache;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import ru.glaizier.key.value.cache3.cache.strategy.Strategy;
import ru.glaizier.key.value.cache3.storage.Storage;

/**
 * Simple cache that updates strategy's statistics on get and put.
 * To make a concurrent SimpleCache, the same strategy as in ConcurrentLruStrategy can be used. In this case,
 * a new method 'peek' in Cache is needed
 *
 * @author GlaIZier
 */

@NotThreadSafe
public class SimpleCache<K, V> implements Cache<K, V> {

    private final Storage<K, V> storage;

    private final Strategy<K> strategy;

    private final int capacity;

    public SimpleCache(Storage<K, V> storage, Strategy<K> strategy, int capacity) {
        if (capacity <= 0)
            throw new IllegalArgumentException("Capacity can't be less than 1!");
        this.storage = storage;
        this.strategy = strategy;
        this.capacity = capacity;
    }

    @Override
    public Optional<V> get(@Nonnull K key) {
        // update statistics only if this key is present in the storage
        return storage.get(key)
            .map(v -> {
                strategy.use(key);
                return v;
            });
    }

    @Override
    public Optional<Map.Entry<K, V>> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        Optional<Map.Entry<K, V>> evicted = Optional.empty();
        if (isFull() && !contains(key)) {
            evicted = evict();
        }

        strategy.use(key);
        storage.put(key, value);
        return evicted;
    }

    @Override
    public Optional<Map.Entry<K, V>> evict() {
        return strategy.evict()
                .map(evictedKey -> {
                    V evictedValue = storage.remove(evictedKey).orElseThrow(IllegalStateException::new);
                    return new AbstractMap.SimpleImmutableEntry<>(evictedKey, evictedValue);
                });
    }

    @Override
    public Optional<V> remove(@Nonnull K key) {
        strategy.remove(key);
        return storage.remove(key);
    }

    @Override
    public boolean contains(@Nonnull K key) {
        return storage.contains(key);
    }

    @Override
    public int getSize() {
        return storage.getSize();
    }

    @Override
    public int getCapacity() {
        return capacity;
    }
}

package ru.glaizier.key.value.cache3.cache;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import ru.glaizier.key.value.cache3.storage.StorageException;

/**
 * @author GlaIZier
 */
@ThreadSafe
public class SynchronizedCache<K, V> implements Cache<K, V> {

    @GuardedBy("lock")
    private final Cache<K, V> cache;

    private final Object lock = new Object();

    public SynchronizedCache(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public Optional<V> get(@Nonnull K key) throws StorageException {
        synchronized (lock){
            return cache.get(key);
        }
    }

    @Override
    public Optional<V> remove(@Nonnull K key) throws StorageException {
        synchronized (lock){
            return cache.remove(key);
        }
    }

    @Override
    public boolean contains(@Nonnull K key) throws StorageException {
        synchronized (lock){
            return cache.contains(key);
        }
    }

    @Override
    public int getSize() {
        synchronized (lock){
            return cache.getSize();
        }
    }

    @Override
    public Optional<Map.Entry<K, V>> put(@Nonnull K key, @Nonnull V value) {
        synchronized (lock){
            return cache.put(key, value);
        }
    }

    @Override
    public Optional<Map.Entry<K, V>> evict() {
        synchronized (lock){
            return cache.evict();
        }
    }

    @Override
    public int getCapacity() {
        synchronized (lock){
            return cache.getCapacity();
        }
    }

}

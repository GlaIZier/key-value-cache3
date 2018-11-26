package ru.glaizier.key.value.cache3.storage;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * @author GlaIZier
 */
@ThreadSafe
public class SynchronousStorage<K, V> implements Storage<K, V> {

    private final Object lock = new Object();

    @GuardedBy("lock")
    private final Storage<K, V> storage;

    public SynchronousStorage(Storage<K, V> storage) {
        this.storage = storage;
    }

    @Override
    public Optional<V> get(@Nonnull K key) throws StorageException {
        Objects.requireNonNull(key);

        synchronized (lock) {
            return storage.get(key);
        }
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) throws StorageException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        synchronized (lock) {
            return storage.put(key, value);
        }
    }


    @Override
    public Optional<V> remove(@Nonnull K key) throws StorageException {
        Objects.requireNonNull(key);

        synchronized (lock) {
            return storage.remove(key);
        }
    }

    @Override
    public boolean contains(@Nonnull K key) throws StorageException {
        Objects.requireNonNull(key);

        synchronized (lock) {
            return storage.contains(key);
        }
    }

    @Override
    public int getSize() {
        synchronized (lock) {
            return storage.getSize();
        }
    }

}

package ru.glaizier.key.value.cache3.storage;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Interface for a key-value storage
 * @author GlaIZier
 */
public interface Storage<K, V> extends RestrictedStorage<K, V> {

    /**
     * @return previous value or empty if there was no such key before
     */
    Optional<V> put(@Nonnull K key, @Nonnull V value) throws StorageException;

}

package ru.glaizier.key.value.cache3.storage;

import ru.glaizier.key.value.cache3.storage.exception.StorageException;

import javax.annotation.Nonnull;
import java.util.Optional;

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

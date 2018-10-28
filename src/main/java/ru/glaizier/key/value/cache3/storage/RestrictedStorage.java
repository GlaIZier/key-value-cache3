package ru.glaizier.key.value.cache3.storage;

import ru.glaizier.key.value.cache3.storage.exception.StorageException;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * @author GlaIZier
 */
public interface RestrictedStorage<K, V> {

    Optional<V> get(@Nonnull K key) throws StorageException;

    /**
     * @return removed value or empty if the key was not found.
     */
    Optional<V> remove(@Nonnull K key) throws StorageException;

    boolean contains(@Nonnull K key) throws StorageException;

    /**
     * @return current number of elements
     */
    int getSize();

    default boolean isEmpty() {
        return getSize() == 0;
    }

}

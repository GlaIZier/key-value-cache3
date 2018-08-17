package ru.glaizier.key.value.cache3.storage;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * @author GlaIZier
 */
public interface RestrictedMap<K, V> {

    Optional<V> get(@Nonnull K key);

    /**
     * @return removed value or empty if the key was not found.
     */
    Optional<V> remove(@Nonnull K key);

    boolean contains(@Nonnull K key);

    /**
     * @return current number of elements
     */
    int getSize();

    default boolean isEmpty() {
        return getSize() == 0;
    }

}

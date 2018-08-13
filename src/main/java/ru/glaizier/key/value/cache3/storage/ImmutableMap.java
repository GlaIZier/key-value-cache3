package ru.glaizier.key.value.cache3.storage;

import java.util.Optional;

/**
 * @author GlaIZier
 */
public interface ImmutableMap<K, V> {

    Optional<V> get(K key);

    /**
     * @return previous value or empty if there was no such value. Or return Optional.empty when no such key.
     * Use contains() to handle this situation
     */
    Optional<V> remove(K key);

    boolean contains(K key);

    /**
     * @return current number of elements
     */
    int getSize();

    default boolean isEmpty() {
        return getSize() == 0;
    }

}

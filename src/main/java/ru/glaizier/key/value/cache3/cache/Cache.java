package ru.glaizier.key.value.cache3.cache;

import java.util.Map;
import java.util.Optional;

import ru.glaizier.key.value.cache3.storage.ImmutableMap;

/**
 * @author GlaIZier
 */
public interface Cache<K, V> extends ImmutableMap<K, V> {

    /**
     * Put the element to the cache and get evicted element if exists
     * @param key
     * @param value
     * @return
     */
    Optional<Map.Entry<K, V>> put(K key, V value);

    /**
     * Removes first candidate to remove from cache
     *
     * @return key-value of removed candidate
     */
    Optional<Map.Entry<K, V>> evict();

    int getCapacity();

    default boolean isFull() {
        return getSize() == getCapacity();
    }

}

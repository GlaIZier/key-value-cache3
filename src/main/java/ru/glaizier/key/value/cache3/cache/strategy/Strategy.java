package ru.glaizier.key.value.cache3.cache.strategy;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Interface for cache strategy
 *
 * @author GlaIZier
 */
public interface Strategy<K> {

    /**
     * Removes statistics for such key and evicts the first suited candidate
     *
     * @return evicted key
     */
    Optional<K> evict();

    /**
     * Tells the strategy that current key element was recently used. Also, can be used to add new key to statistics
     *
     * @return true if statistics has already existed for this element (the element is not new)
     */
    boolean use(@Nonnull K key);

    /**
     * Removes key from statistics
     *
     * @return true if such key was removed
     */
    boolean remove(@Nonnull K key);

}

package ru.glaizier.key.value.cache3.cache.strategy;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @author GlaIZier
 */
@NotThreadSafe
public class LruStrategy<K> implements Strategy<K> {

    /**
     * We need to be able to get by key, replace elements and get first in queue in O(1).
     * This can be achieved by Java SE means. This set starts iteration from the last added element
     */
    private final Set<K> queue = new LinkedHashSet<>();

    /**
     * O(1)
     */
    @Override
    public Optional<K> evict() {
        // Find first element for eviction and remove it if it was found
        return queue.stream()
                .findFirst()
                .map(evictedKey -> {
                    queue.remove(evictedKey);
                    return evictedKey;
                });
    }

    /**
     * O(1)
     */
    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        boolean contained = queue.remove(key);
        queue.add(key);
        return contained;
    }

    /**
     * O(1)
     */
    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return queue.remove(key);
    }
}

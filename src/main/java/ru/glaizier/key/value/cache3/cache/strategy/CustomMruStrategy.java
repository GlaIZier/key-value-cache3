package ru.glaizier.key.value.cache3.cache.strategy;

import ru.glaizier.key.value.cache3.util.LinkedHashSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;

/**
 * Most recently used. First eviction candidate is a candidate who has been used recently.
 * This implementation is similar to MruStrategy, but here my own LinkedHashSet is used.
 *
 * @author GlaIZier
 */
public class CustomMruStrategy<K> implements Strategy<K> {

    /**
     * We need to be able to get by key, replace elements and get first in queue in O(1).
     * This can't be achieved by Java SE. To get it done, my own implementation is used.
     */
    private final LinkedHashSet<K> queue = new LinkedHashSet<>();

    @Override
    public Optional<K> evict() {
        // remove from queue if found key
        return Optional.ofNullable(queue.getHead())
                .map(evicted -> {
                    queue.remove(evicted);
                    return evicted;
                });
    }

    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        boolean contained = queue.remove(key);
        queue.addToHead(key);
        return contained;
    }

    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return queue.remove(key);
    }
}

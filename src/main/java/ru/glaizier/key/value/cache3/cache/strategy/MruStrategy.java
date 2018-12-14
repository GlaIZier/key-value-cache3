package ru.glaizier.key.value.cache3.cache.strategy;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.collections4.map.LinkedMap;

/**
 * @author GlaIZier
 */
@NotThreadSafe
// Todo Add my LinkedHashSet implementation and another MRUstrategy
public class MruStrategy<K> implements Strategy<K> {

    // as we don't need map, this object is saved as values
    private static final Object DUMMY = new Object();

    /**
     * We need to be able to get by key, replace elements and get a last element in queue in O(1).
     * This can be achieved by Collections4 library.
     */
    private final LinkedMap<K, Object> queue = new LinkedMap<>();

    @Override
    public Optional<K> evict() {
        if (queue.isEmpty())
            return empty();
        K lastAdded = queue.lastKey();
        return of(queue.remove(lastAdded)).map(v -> lastAdded);
    }

    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        boolean contained = ofNullable(queue.remove(key)).isPresent();
        queue.put(key, DUMMY);
        return contained;
    }

    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        return ofNullable(queue.remove(key)).isPresent();
    }
}

package ru.glaizier.key.value.cache3.cache.strategy;

import org.apache.commons.collections4.map.LinkedMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.*;

/**
 * @author GlaIZier
 */
@NotThreadSafe
public class MruStrategy<K> implements Strategy<K> {

    // as we don't need map, this object is saved as values
    private static final Object DUMMY = new Object();

    /**
     * We need to be able to get by key, replace elements and get a last element in queue in O(1).
     * This can be achieved by using Collections4 library.
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

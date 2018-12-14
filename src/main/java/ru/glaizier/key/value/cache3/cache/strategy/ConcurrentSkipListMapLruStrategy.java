package ru.glaizier.key.value.cache3.cache.strategy;

import static java.lang.Math.toIntExact;
import static java.util.Optional.ofNullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * @author GlaIZier
 */

// Doesn't work. Can we somehow use this idea to come up with more concurrent Strategy?
@Deprecated
public class ConcurrentSkipListMapLruStrategy<K> implements Strategy<K> {

    // as we don't need map, this object is saved as values
    private static final Object DUMMY = new Object();

    @ThreadSafe
    @Immutable
    private static class Node<K> implements Comparable<Node<K>> {
        private final long timestamp;
        private final K key;

        private Node(long timestamp, @Nonnull K key) {
            Objects.requireNonNull(key);
            this.timestamp = timestamp;
            this.key = key;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Node && key.equals(((Node) obj).getKey());
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        private long getTimestamp() {
            return timestamp;
        }

        private K getKey() {
            return key;
        }

        @Override
        public int compareTo(@Nonnull Node<K> o) {
            return toIntExact(timestamp - o.getTimestamp());
        }
    }

    private final ConcurrentNavigableMap<Node<K>, Object> queue = new ConcurrentSkipListMap<>();

    @Override
    public Optional<K> evict() {
        return ofNullable(queue.pollLastEntry())
            .map(Map.Entry::getKey)
            .map(Node::getKey);
    }

    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key);
        Object put = queue.put(new Node<>(System.currentTimeMillis(), key), DUMMY);
        return ofNullable(put).isPresent();
    }

    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key);
        long notUsedTimestamp = System.currentTimeMillis();
        Optional<Object> remove = ofNullable(queue.remove(new Node<>(notUsedTimestamp, key)));
        return remove.isPresent();
    }
}

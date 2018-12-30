package ru.glaizier.key.value.cache3.cache;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.util.*;

import static java.lang.String.format;

/**
 * Multi level cache implementation which evicts elements from first (top) levels to below ones.
 * Equal keys can't be present in different levels
 */
@NotThreadSafe
public class MultiLevelCache<K extends Serializable, V extends Serializable> implements Cache<K, V> {

    private final List<Cache<K, V>> levels;

    @SafeVarargs
    public MultiLevelCache(Cache<K, V>... levels) {
        this(Arrays.asList(levels));
    }

    public MultiLevelCache(List<Cache<K, V>> levels) {
        Objects.requireNonNull(levels, "levels");
        if (levels.isEmpty()) {
            throw new IllegalArgumentException("Levels must not be empty!");
        }
        this.levels = Collections.unmodifiableList(levels);
    }

    /**
     * Searches key in all levels and puts found to the first level
     */
    @Override
    public Optional<V> get(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        Optional<V> foundOpt = levels.stream()
                .map(cache -> cache.get(key))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
        // move element to the first level
        return foundOpt.map(foundValue -> {
            remove(key);
            put(key, foundValue).ifPresent(evicted -> {
                throw new IllegalStateException(format("Element %s-%s has been evicted during get method!",
                        evicted.getKey(), evicted.getValue()));
            });
            return foundValue;
        });
    }

    /**
     * Puts to the first level and evicts consequently
     */
    @Override
    public Optional<Map.Entry<K, V>> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        // Removes the key if it already in the cache
        remove(key);
        return putRec(key, value, 0);
    }

    /**
     * Evicts consequently from levels by putting evicted elements to other levels
     * l0 -> ev0 + l1 -> ev1 + l2 -> el2 ...
     */
    @Override
    public Optional<Map.Entry<K, V>> evict() {
        return levels.get(0).evict().flatMap(firstEvicted -> putRec(firstEvicted.getKey(), firstEvicted.getValue(), 1));
    }

    /**
     * Puts the element to the start level and gets the evicted from the last level
     */
    private Optional<Map.Entry<K, V>> put(K key, V value, int startLevelIndex) {
        Optional<Map.Entry<K, V>> curEvicted = Optional.of(new AbstractMap.SimpleImmutableEntry<>(key, value));
        for (int levelIndex = startLevelIndex; levelIndex < levels.size() && curEvicted.isPresent(); levelIndex++) {
            curEvicted = levels.get(levelIndex).put(curEvicted.get().getKey(), curEvicted.get().getValue());
        }
        return curEvicted;
    }

    /**
     * Puts recursively the element to the start level and gets the evicted from the last level
     */
    private Optional<Map.Entry<K, V>> putRec(K key, V value, int curLevelIndex) {
        if (curLevelIndex == levels.size())
            return Optional.of(new AbstractMap.SimpleImmutableEntry<>(key, value));
        Optional<Map.Entry<K, V>> curEvictedOpt = levels.get(curLevelIndex).put(key, value);
        return curEvictedOpt.flatMap(curEvicted -> putRec(curEvicted.getKey(), curEvicted.getValue(), curLevelIndex + 1));
    }

    /**
     * Removes the element from the first found level.
     */
    @Override
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return levels.stream()
                .filter(level -> level.contains(key))
                .findFirst()
                .flatMap(level -> level.remove(key));
    }

    @Override
    public boolean contains(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return levels.stream().anyMatch(level -> level.contains(key));
    }

    @Override
    public int getSize() {
        return levels.stream()
                .mapToInt(Cache::getSize)
                .reduce(0, Integer::sum);
    }

    @Override
    public int getCapacity() {
        return levels.stream()
                .mapToInt(Cache::getCapacity)
                .reduce(0, Integer::sum);
    }
}

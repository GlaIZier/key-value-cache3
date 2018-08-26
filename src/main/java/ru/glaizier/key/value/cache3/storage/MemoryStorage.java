package ru.glaizier.key.value.cache3.storage;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;


public class MemoryStorage<K, V> implements Storage<K, V> {

    private final Map<K, V> map;

    public static <K, V> MemoryStorage<K, V> ofHashMap() {
        return new MemoryStorage<>(new HashMap<>());
    }

    public MemoryStorage(Map<K, V> map) {
        Objects.requireNonNull(map, "map");
        this.map = map;
    }

    @Override
    public Optional<V> get(@Nonnull K key) {
        return ofNullable(map.get(key));
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key);
        return ofNullable(map.put(key, value));
    }

    @Override
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key);
        return ofNullable(map.remove(key));
    }

    @Override
    public boolean contains(@Nonnull K key) {
        Objects.requireNonNull(key);
        return map.containsKey(key);
    }

    @Override
    public int getSize() {
        return map.size();
    }

}

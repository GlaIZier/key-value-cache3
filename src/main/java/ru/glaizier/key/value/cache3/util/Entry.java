package ru.glaizier.key.value.cache3.util;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Entry<K, V> implements Serializable {
    private static final long serialVersionUID = 4865908910190150321L;

    public final K key;
    public final V value;
    public Entry(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        this.key = key;
        this.value = value;
    }
}

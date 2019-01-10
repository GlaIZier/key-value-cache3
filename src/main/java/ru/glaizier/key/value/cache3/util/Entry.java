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

    @Override
    public int hashCode() {
        // name's hashCode is multiplied by an arbitrary prime number (13)
        // in order to make sure there is a difference in the hashCode between
        // these two parameters:
        //  name: a  value: aa
        //  name: aa value: a
        return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof Entry) {
            Entry entry = (Entry) o;
            if (key != null ? !key.equals(entry.key) : entry.key != null) return false;
            return value != null ? value.equals(entry.value) : entry.value == null;
        }
        return false;
    }
}

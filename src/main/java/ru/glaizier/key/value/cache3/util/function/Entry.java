package ru.glaizier.key.value.cache3.util.function;

import java.io.Serializable;

import jdk.nashorn.internal.ir.annotations.Immutable;

@Immutable
public class Entry<K, V> implements Serializable {
    private static final long serialVersionUID = 4865908910190150321L;

    public final K key;
    public final V value;
    public Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }
}

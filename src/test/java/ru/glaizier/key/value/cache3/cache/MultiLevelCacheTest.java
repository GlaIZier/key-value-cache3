package ru.glaizier.key.value.cache3.cache;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ru.glaizier.key.value.cache3.cache.strategy.LruStrategy;
import ru.glaizier.key.value.cache3.storage.file.FileStorage;
import ru.glaizier.key.value.cache3.storage.memory.MemoryStorage;

import java.util.AbstractMap;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class MultiLevelCacheTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Cache<Integer, String> c;

    @Before
    public void init() {
        c = new MultiLevelCache<>(
                new SimpleCache<>(new MemoryStorage<>(), new LruStrategy<>(), 2),
                new SimpleCache<>(new FileStorage<>(temporaryFolder.getRoot().toPath()), new LruStrategy<>(), 2)
        );
    }

    @Test
    public void put() {
        assertFalse(c.contains(1));
        assertThat(c.get(1), is(Optional.empty()));
        assertFalse(c.contains(2));
        assertThat(c.get(2), is(Optional.empty()));
        assertFalse(c.contains(3));
        assertThat(c.get(3), is(Optional.empty()));
        assertFalse(c.contains(4));
        assertThat(c.get(4), is(Optional.empty()));

        assertThat(c.put(1, "1"), is(Optional.empty()));

        assertTrue(c.contains(1));
        assertThat(c.get(1), is(Optional.of("1")));

        assertThat(c.put(2, "2"), is(Optional.empty()));

        assertTrue(c.contains(2));
        assertThat(c.get(2), is(Optional.of("2")));

        assertThat(c.put(3, "3"), is(Optional.empty()));

        assertTrue(c.contains(3));
        assertThat(c.get(3), is(Optional.of("3")));

        assertThat(c.put(4, "4"), is(Optional.empty()));

        assertTrue(c.contains(4));
        assertThat(c.get(4), is(Optional.of("4")));

        assertThat(c.put(5, "5"), is(Optional.of(new AbstractMap.SimpleImmutableEntry<>(1, "1"))));

        assertTrue(c.contains(5));
        assertThat(c.get(5), is(Optional.of("5")));
        assertFalse(c.contains(1));
        assertThat(c.get(1), is(Optional.empty()));

        assertThat(c.put(2, "22"), is(Optional.empty()));
        assertTrue(c.contains(2));
        assertThat(c.get(2), is(Optional.of("22")));
    }

    @Test
    public void remove() {
        assertThat(c.remove(1), is(Optional.empty()));
        c.put(1, "1");
        assertThat(c.getSize(), is(1));
        assertThat(c.remove(1), is(Optional.of("1")));
        assertThat(c.getSize(), is(0));
        assertThat(c.get(1), is(Optional.empty()));

        c.put(1, "1");
        c.evict();
        assertThat(c.getSize(), is(1));
        assertThat(c.remove(1), is(Optional.of("1")));
        assertThat(c.getSize(), is(0));
        assertThat(c.get(1), is(Optional.empty()));

        c.put(1, "1");
        c.put(2, "2");
        c.put(3, "3");
        c.put(4, "4");
        assertThat(c.getSize(), is(4));
        assertThat(c.remove(1), is(Optional.of("1")));
        assertThat(c.remove(3), is(Optional.of("3")));
        c.evict();
        assertThat(c.remove(2), is(Optional.of("2")));
        assertThat(c.remove(4), is(Optional.of("4")));
        assertThat(c.getSize(), is(0));
        assertThat(c.get(1), is(Optional.empty()));
    }

    @Test
    public void contains() {
        assertFalse(c.contains(1));
        assertFalse(c.contains(2));
        assertFalse(c.contains(3));
        assertFalse(c.contains(4));
        c.put(1, "1");
        assertTrue(c.contains(1));
        c.put(2, "2");
        assertTrue(c.contains(2));
        c.put(3, "3");
        assertTrue(c.contains(3));
        c.put(4, "4");
        assertTrue(c.contains(4));
        c.put(5, "5");
        assertTrue(c.contains(5));
        assertFalse(c.contains(1));
    }


    @Test
    public void notEvictIfFirstLevelEmpty() {
        assertThat(c.evict(), is(Optional.empty()));
        c.put(1, "1");
        assertThat(c.evict(), is(Optional.empty()));
        assertThat(c.evict(), is(Optional.empty()));
    }

    @Test
    public void evict() {
        assertThat(c.evict(), is(Optional.empty()));
        // 1 <> - <> <>
        c.put(1, "1");
        // evict to the second
        // <> <> - 1 <>
        assertThat(c.evict(), is(Optional.empty()));
        // add to the first level
        // 2 <> - 1 <>
        c.put(2, "2");
        // evict to the second
        // <> <> - 2 1
        assertThat(c.evict(), is(Optional.empty()));
        // add to the first
        // 3 <> - 2 1
        c.put(3, "3");
        // evict to the second
        // <> <> - 3 2
        assertThat(c.evict(), is(Optional.of(new AbstractMap.SimpleImmutableEntry<>(1, "1"))));
        assertThat(c.evict(), is(Optional.empty()));
        c.put(4, "4");
        c.put(5, "5");
        // 5 4 - 3 2
        assertThat(c.evict(), is(Optional.of(new AbstractMap.SimpleImmutableEntry<>(2, "2"))));
        // <> 5 - 4 3
        assertThat(c.evict(), is(Optional.of(new AbstractMap.SimpleImmutableEntry<>(3, "3"))));
        // <> <> - 5 4
        assertThat(c.evict(), is(Optional.empty()));
    }

    @Test
    public void get() {
        assertThat(c.get(1), is(Optional.empty()));
        c.put(1, "1");
        assertThat(c.get(1), is(Optional.of("1")));
        c.put(2, "1");
        assertThat(c.getSize(), is(2));
        assertThat(c.get(1), is(Optional.of("1")));
        assertThat(c.get(2), is(Optional.of("1")));
        c.put(3, "1");
        assertThat(c.getSize(), is(3));
        assertThat(c.get(3), is(Optional.of("1")));
        c.put(4, "1");
        assertThat(c.getSize(), is(4));
        assertThat(c.get(4), is(Optional.of("1")));
        c.put(5, "5");
        assertThat(c.getSize(), is(4));
        assertThat(c.get(1), is(Optional.empty()));
    }

    @Test
    public void isFull() {
        assertFalse(c.isFull());
        c.put(1, "1");
        assertFalse(c.isFull());
        c.put(2, "1");
        assertFalse(c.isFull());
        c.put(3, "1");
        assertFalse(c.isFull());
        c.put(4, "1");
        assertTrue(c.isFull());
    }

    @Test
    public void capacity() {
        assertThat(c.getCapacity(), is(4));
        c.put(1, "1");
        assertThat(c.getCapacity(), is(4));
    }


    @Test
    public void size() {
        assertThat(c.getSize(), is(0));
        c.put(1, "1");
        assertThat(c.getSize(), is(1));
        c.put(2, "2");
        assertThat(c.getSize(), is(2));
        c.put(2, "3");
        assertThat(c.getSize(), is(2));
        c.put(3, "3");
        assertThat(c.getSize(), is(3));
        c.put(4, "4");
        assertThat(c.getSize(), is(4));
        c.put(5, "5");
        assertThat(c.getSize(), is(4));
    }

}

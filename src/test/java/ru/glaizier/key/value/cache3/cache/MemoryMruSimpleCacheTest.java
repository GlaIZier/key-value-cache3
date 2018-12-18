package ru.glaizier.key.value.cache3.cache;

import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import ru.glaizier.key.value.cache3.cache.strategy.MruStrategy;
import ru.glaizier.key.value.cache3.storage.memory.MemoryStorage;

/**
 * Here it could be right to use mocks to unit test Cache itself,
 * but I don't want additional Maven dependency in this small project. So, this is a kind of integration test.
 * @author GlaIZier
 */

public class MemoryMruSimpleCacheTest extends SimpleCacheTest{

    private final Cache<Integer, String> cache = new SimpleCache<>(new MemoryStorage<>(), new MruStrategy<>(), 2);

    @Override
    protected Cache<Integer, String> getCache() {
        return cache;
    }

    @Test
    public void afterOneTwoInsertsChecks() {
        assertThat(cache.put(1, "1"), is(Optional.empty()));
        assertThat(cache.put(2, "2"), is(Optional.empty()));

        assertTrue(cache.isFull());
        assertThat(cache.getCapacity(), is(2));
        assertThat(cache.getSize(), is(2));
        assertThat(cache.get(1), is(Optional.of("1")));
        assertThat(cache.get(2), is(Optional.of("2")));
        assertThat(cache.evict().get().getValue(), is("2"));
        assertThat(cache.evict().get().getValue(), is("1"));
    }

    @Test
    public void afterOneTwoThreeInsertsAndOneUpdate() {
        assertThat(cache.put(1, "1"), is(Optional.empty()));
        assertThat(cache.put(2, "2"), is(Optional.empty()));
        assertThat(cache.get(1), is(Optional.of("1")));
        assertThat(cache.put(3, "3").get().getValue(), is("1"));

        assertTrue(cache.isFull());
        assertThat(cache.getCapacity(), is(2));
        assertThat(cache.getSize(), is(2));
        assertThat(cache.get(1), is(Optional.empty()));
        assertThat(cache.get(2), is(Optional.of("2")));
        assertThat(cache.get(3), is(Optional.of("3")));
        assertThat(cache.evict().get().getValue(), is("3"));
        assertThat(cache.evict().get().getValue(), is("2"));
    }
}

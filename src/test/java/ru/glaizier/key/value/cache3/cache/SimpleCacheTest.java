package ru.glaizier.key.value.cache3.cache;

import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * @author GlaIZier
 */
public abstract class SimpleCacheTest {

    protected abstract Cache<Integer, String> getCache();

    @Test
    public void emptyCacheChecks() {
        assertFalse(getCache().isFull());
        assertThat(getCache().getCapacity(), is(2));
        assertThat(getCache().getSize(), is(0));
        assertThat(getCache().evict(), is(Optional.empty()));
        assertThat(getCache().get(1), is(Optional.empty()));
    }

    @Test
    public void afterOneInsertChecks() {
        assertThat(getCache().put(1, "1"), is(Optional.empty()));

        assertFalse(getCache().isFull());
        assertThat(getCache().getCapacity(), is(2));
        assertThat(getCache().getSize(), is(1));
        assertThat(getCache().get(1), is(Optional.of("1")));
        assertThat(getCache().evict().get().getValue(), is("1"));
    }

    @Test
    public void removeContains() {
        assertFalse(getCache().contains(1));
        assertFalse(getCache().contains(2));
        getCache().put(1, "1");
        getCache().put(2, "2");
        assertTrue(getCache().contains(1));
        assertTrue(getCache().contains(2));
        getCache().remove(2);
        getCache().remove(1);
        assertFalse(getCache().contains(1));
        assertFalse(getCache().contains(2));
    }

}

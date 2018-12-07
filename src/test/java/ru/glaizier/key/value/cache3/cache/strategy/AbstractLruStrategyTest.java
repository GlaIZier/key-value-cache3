package ru.glaizier.key.value.cache3.cache.strategy;

import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * @author GlaIZier
 */
public abstract class AbstractLruStrategyTest extends AbstractStrategyTest {

    @Test
    public void getEmptyOnEmptyQueue() {
        assertThat(getStrategy().evict(), is(Optional.empty()));
    }

    @Test
    public void getOneAfterOneInsert() {
        assertFalse(getStrategy().use(1));
        assertThat(getStrategy().evict(), is(Optional.of(1)));
    }

    @Test
    public void getOneAfterOneTwoInserts() {
        assertFalse(getStrategy().use(1));
        assertFalse(getStrategy().use(2));
        assertThat(getStrategy().evict(), is(Optional.of(1)));
    }

    @Test
    public void getTwoAfterOneTwoInsertsAndOneUpdate() {
        assertFalse(getStrategy().use(1));
        assertFalse(getStrategy().use(2));
        assertTrue(getStrategy().use(1));
        assertThat(getStrategy().evict(), is(Optional.of(2)));
    }

}

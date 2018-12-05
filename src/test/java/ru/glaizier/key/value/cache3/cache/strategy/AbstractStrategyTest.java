package ru.glaizier.key.value.cache3.cache.strategy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * @author GlaIZier
 */
public abstract class AbstractStrategyTest {

    protected abstract Strategy<Integer> getStrategy();

    @Test
    public void remove(){
        getStrategy().use(1);
        getStrategy().use(2);
        assertTrue(getStrategy().remove(1));
        assertFalse(getStrategy().remove(1));
        assertTrue(getStrategy().remove(2));
        assertFalse(getStrategy().remove(2));
    }
}

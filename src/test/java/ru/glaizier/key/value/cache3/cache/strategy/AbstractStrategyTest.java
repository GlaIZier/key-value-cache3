package ru.glaizier.key.value.cache3.cache.strategy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * @author GlaIZier
 */
public abstract class AbstractStrategyTest {

    protected Strategy<Integer> strategy;

    protected abstract Strategy<Integer> getStrategy();

    @Before
    public void init() {
        strategy = getStrategy();
    }

    @Test
    public void remove(){
        strategy.use(1);
        strategy.use(2);
        assertTrue(strategy.remove(1));
        assertFalse(strategy.remove(1));
        assertTrue(strategy.remove(2));
        assertFalse(strategy.remove(2));
    }
}

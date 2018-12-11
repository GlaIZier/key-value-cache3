package ru.glaizier.key.value.cache3.cache.strategy;

/**
 * @author GlaIZier
 */
public class LruStrategyTest extends AbstractLruStrategyTest {
    @Override
    protected Strategy<Integer> getStrategy() {
        return new LruStrategy<>();
    }
}

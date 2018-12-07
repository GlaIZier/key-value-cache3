package ru.glaizier.key.value.cache3.cache.strategy;

/**
 * @author GlaIZier
 */
public class SynchronizedLruStrategyTest extends AbstractLruStrategyTest {
    private final Strategy<Integer> strategy = new LruStrategy<>();

    @Override
    protected Strategy<Integer> getStrategy() {
        return strategy;
    }
}

package ru.glaizier.key.value.cache3.cache.strategy;

/**
 * @author GlaIZier
 */
public class MruStrategyTest extends AbstractMruStrategyTest {
    @Override
    protected Strategy<Integer> getStrategy() {
        return new MruStrategy<>();
    }
}

package ru.glaizier.key.value.cache3.cache.strategy;

/**
 * @author GlaIZier
 */
public class CustomMruStrategyTest extends AbstractMruStrategyTest {
    @Override
    protected Strategy<Integer> getStrategy() {
        return new CustomMruStrategy<>();
    }
}

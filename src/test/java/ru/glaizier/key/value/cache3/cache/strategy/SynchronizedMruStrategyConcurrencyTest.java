package ru.glaizier.key.value.cache3.cache.strategy;

/**
 * @author GlaIZier
 */
public class SynchronizedMruStrategyConcurrencyTest extends AbstractStrategyConcurrencyTest {

    @Override
    protected Strategy<Integer> getStrategy() {
        return new SynchronizedStrategy<>(new MruStrategy<>());
    }
}

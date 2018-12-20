package ru.glaizier.key.value.cache3.cache.strategy;

/**
 * @author GlaIZier
 */
public class SynchronizedCustomMruStrategyConcurrencyTest extends AbstractStrategyConcurrencyTest {

    @Override
    protected Strategy<Integer> getStrategy() {
        return new SynchronizedStrategy<>(new CustomMruStrategy<>());
    }
}

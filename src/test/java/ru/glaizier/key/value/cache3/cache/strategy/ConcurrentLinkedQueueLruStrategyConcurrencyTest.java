package ru.glaizier.key.value.cache3.cache.strategy;

/**
 * @author GlaIZier
 */
public class ConcurrentLinkedQueueLruStrategyConcurrencyTest extends AbstractStrategyConcurrencyTest {

    @Override
    protected Strategy<Integer> getStrategy() {
        return new ConcurrentLinkedQueueLruStrategy<>();
    }
}

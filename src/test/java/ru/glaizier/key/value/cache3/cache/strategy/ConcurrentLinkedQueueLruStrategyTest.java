package ru.glaizier.key.value.cache3.cache.strategy;


public class ConcurrentLinkedQueueLruStrategyTest extends AbstractLruStrategyTest {
    private final Strategy<Integer> strategy = new ConcurrentLinkedQueueLruStrategy<>();

    @Override
    protected Strategy<Integer> getStrategy() {
        return strategy;
    }
}

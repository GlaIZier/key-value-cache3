package ru.glaizier.key.value.cache3.cache.strategy;


public class ConcurrentLruStrategy1Test extends AbstractLruStrategyTest {
    private final Strategy<Integer> strategy = new ConcurrentLruStrategy<>();

    @Override
    protected Strategy<Integer> getStrategy() {
        return strategy;
    }
}

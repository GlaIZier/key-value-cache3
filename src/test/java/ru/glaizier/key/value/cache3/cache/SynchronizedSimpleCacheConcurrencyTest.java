package ru.glaizier.key.value.cache3.cache;

import ru.glaizier.key.value.cache3.cache.strategy.ConcurrentLruStrategy;
import ru.glaizier.key.value.cache3.storage.memory.MemoryStorage;

/**
 * @author GlaIZier
 */
public class SynchronizedSimpleCacheConcurrencyTest extends AbstractCacheConcurrencyTest {

    @Override
    protected Cache<Integer, Integer> getCache() {
        return new SynchronizedCache<>(new SimpleCache<>(new MemoryStorage<>(), new ConcurrentLruStrategy<>(), AbstractCacheConcurrencyTest.TASKS_NUMBER));
    }
}

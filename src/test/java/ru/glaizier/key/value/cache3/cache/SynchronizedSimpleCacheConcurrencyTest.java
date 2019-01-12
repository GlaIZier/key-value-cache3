package ru.glaizier.key.value.cache3.cache;

import ru.glaizier.key.value.cache3.cache.strategy.ConcurrentLinkedQueueLruStrategy;
import ru.glaizier.key.value.cache3.storage.memory.MemoryStorage;

/**
 * @author GlaIZier
 */
public class SynchronizedSimpleCacheConcurrencyTest extends AbstractCacheConcurrencyTest {

    @Override
    protected Cache<Integer, Integer> getCache(int capacity) {
        return new SynchronizedCache<>(new SimpleCache<>(new MemoryStorage<>(), new ConcurrentLinkedQueueLruStrategy<>(), capacity));
    }
}

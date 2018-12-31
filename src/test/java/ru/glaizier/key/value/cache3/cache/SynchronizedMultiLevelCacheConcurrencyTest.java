package ru.glaizier.key.value.cache3.cache;

import org.junit.Ignore;
import ru.glaizier.key.value.cache3.cache.strategy.ConcurrentLruStrategy;
import ru.glaizier.key.value.cache3.storage.memory.MemoryStorage;

/**
 * @author GlaIZier
 */
@Ignore
// Fixme
public class SynchronizedMultiLevelCacheConcurrencyTest extends AbstractCacheConcurrencyTest {

    @Override
    protected Cache<Integer, Integer> getCache(int capacity) {
        SynchronizedCache<Integer, Integer> level1 = new SynchronizedCache<>(new SimpleCache<>(new MemoryStorage<>(), new ConcurrentLruStrategy<>(), capacity / 2));
        SynchronizedCache<Integer, Integer> level2 = new SynchronizedCache<>(new SimpleCache<>(new MemoryStorage<>(), new ConcurrentLruStrategy<>(), capacity / 2));
        return new SynchronizedCache<>(new MultiLevelCache<>(level1, level2));
    }
}

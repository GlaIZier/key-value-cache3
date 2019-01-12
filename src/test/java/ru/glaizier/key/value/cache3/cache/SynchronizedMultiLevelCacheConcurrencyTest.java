package ru.glaizier.key.value.cache3.cache;

import ru.glaizier.key.value.cache3.cache.strategy.ConcurrentLinkedQueueLruStrategy;
import ru.glaizier.key.value.cache3.storage.memory.MemoryStorage;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

/**
 * @author GlaIZier
 */
public class SynchronizedMultiLevelCacheConcurrencyTest extends AbstractCacheConcurrencyTest {

    @Override
    protected Cache<Integer, Integer> getCache(int capacity) {
        SynchronizedCache<Integer, Integer> level1 = new SynchronizedCache<>(new SimpleCache<>(new MemoryStorage<>(), new ConcurrentLinkedQueueLruStrategy<>(), capacity / 2));
        SynchronizedCache<Integer, Integer> level2 = new SynchronizedCache<>(new SimpleCache<>(new MemoryStorage<>(), new ConcurrentLinkedQueueLruStrategy<>(), capacity / 2));
        return new SynchronizedCache<>(new MultiLevelCache<>(level1, level2));
    }

    @Override
    public void evictPut() throws InterruptedException, ExecutionException {
        Cache<Integer, Integer> cache = getCache(THREADS_NUMBER);
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER);
        // <THREADS_NUMBER / 2> - <>
        IntStream.range(0, THREADS_NUMBER / 2).forEach(i -> cache.put(i, i));

        List<Callable<Object>> putEvictTasks = IntStream.range(THREADS_NUMBER / 2, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                cache.put(threadI, threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        // + THREADS_NUMBER / 2

        AtomicInteger evictMisses = new AtomicInteger();
        List<Callable<Object>> evictTasks = IntStream.range(0, THREADS_NUMBER / 2)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                Optional<Map.Entry<Integer, Integer>> evict = cache.evict();
                if (!evict.isPresent()) {
                    evictMisses.incrementAndGet();
                }
            })
            .map(Executors::callable)
            .collect(toList());
        // - (THREADS_NUMBER / 2 - evictMisses)

        putEvictTasks.addAll(evictTasks);
        Collections.shuffle(putEvictTasks);
        List<Future<Object>> futures = executorService.invokeAll(putEvictTasks);
        // After all operations: from <5> - <5> (5 misses) to <> - <5> (0 misses)

        for (Future<Object> future : futures) {
            future.get();
        }

        for (int i = 0; i < THREADS_NUMBER / 2 + evictMisses.get(); i++) {
            assertThat(cache.evict(), not(Optional.empty()));
        }
        assertThat(cache.evict(), is(Optional.empty()));
        assertThat(cache.isEmpty(), is(true));
    }

    @Override
    public void evictPutRemove() throws InterruptedException, ExecutionException {
        Cache<Integer, Integer> cache = getCache(THREADS_NUMBER * 2);
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER * 2);
        // <THREADS_NUMBER> - <>
        int initialCount = THREADS_NUMBER;
        IntStream.range(0, initialCount).forEach(i -> cache.put(i, i));

        AtomicInteger putWoEvictionCount = new AtomicInteger();
        List<Callable<Object>> putEvictRemoveTasks = IntStream.range(THREADS_NUMBER, THREADS_NUMBER * 2)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                Optional<Map.Entry<Integer, Integer>> put = cache.put(threadI, threadI);
                if (!put.isPresent())
                    putWoEvictionCount.incrementAndGet();
            })
            .map(Executors::callable)
            .collect(toList());
        // + THREADS_NUMBER

        AtomicInteger evictCount = new AtomicInteger();
        List<Callable<Object>> evictTasks = IntStream.range(0, THREADS_NUMBER / 2)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                Optional<Map.Entry<Integer, Integer>> evict = cache.evict();
                if (evict.isPresent())
                    evictCount.incrementAndGet();
            })
            .map(Executors::callable)
            .collect(toList());
        // - evictCount

        AtomicInteger removeCount = new AtomicInteger();
        List<Callable<Object>> removeTasks = IntStream.range(THREADS_NUMBER / 2, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                Optional<Integer> removed = cache.remove(threadI);
                if (removed.isPresent())
                    removeCount.incrementAndGet();
            })
            .map(Executors::callable)
            .collect(toList());
        // - removeCount

        putEvictRemoveTasks.addAll(evictTasks);
        putEvictRemoveTasks.addAll(removeTasks);
        Collections.shuffle(putEvictRemoveTasks);
        List<Future<Object>> futures = executorService.invokeAll(putEvictRemoveTasks);
        // After all operations: from <5> - <5> (5 misses + 5 misses) to <> - <> (0 misses + 0 misses)

        for (Future<Object> future : futures) {
            future.get();
        }

        assertThat(cache.getSize(), is(initialCount + putWoEvictionCount.get() - removeCount.get() - evictCount.get()));
    }
}

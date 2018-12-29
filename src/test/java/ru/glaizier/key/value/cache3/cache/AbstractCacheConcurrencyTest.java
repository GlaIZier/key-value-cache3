package ru.glaizier.key.value.cache3.cache;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author GlaIZier
 */
public abstract class AbstractCacheConcurrencyTest {

    static final int THREADS_NUMBER = 10;

    static final int TASKS_NUMBER = 10;

    private static ExecutorService executorService;

    private Cache<Integer, Integer> cache;

    // Can't init it inline because in this case this executorService will be initialized only once, when the class is
    // loaded by the class loader at the very beginning. So for the next tests executorService will be shut down.
    @BeforeClass
    public static void initStatic() {
        executorService = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void cleanUpClass() throws InterruptedException {
        executorService.shutdownNow();
        if (!executorService.awaitTermination(1, SECONDS)) {
            System.exit(0);
        }
    }

    protected abstract Cache<Integer, Integer> getCache(int capacity);

    @Before
    public void init() {
        cache = getCache(TASKS_NUMBER);
    }

    private Map.Entry<List<Callable<Object>>, Supplier<Integer>> buildRemoveTasks(Cache<Integer, Integer> cache, CyclicBarrier barrier) {
        AtomicInteger removeMisses = new AtomicInteger();
        List<Callable<Object>> removeTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                IntStream.range(0, TASKS_NUMBER)
                    .forEach(taskI -> {
                        Thread.yield();
                        Optional<Integer> removed = cache.remove(threadI * 10 + taskI);
                        if (!removed.isPresent())
                            removeMisses.incrementAndGet();
                    });
            })
            .map(Executors::callable)
            .collect(toList());
        return new AbstractMap.SimpleImmutableEntry<>(removeTasks, removeMisses::get);
    }

    private Map.Entry<List<Callable<Object>>, Supplier<Integer>> buildEvictTasks(Cache<Integer, Integer> cache, CyclicBarrier barrier) {
        AtomicInteger evictMisses = new AtomicInteger();
        List<Callable<Object>> evictTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                IntStream.range(0, TASKS_NUMBER)
                    .forEach(taskI -> {
                        Thread.yield();
                        Optional<Map.Entry<Integer, Integer>> evict = cache.evict();
                        if (!evict.isPresent())
                            evictMisses.incrementAndGet();
                    });
            })
            .map(Executors::callable)
            .collect(toList());
        return new AbstractMap.SimpleImmutableEntry<>(evictTasks, evictMisses::get);
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    // put every element with taskI simultaneously using CyclicBarrier
    public void push() throws InterruptedException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER);
        List<Callable<Object>> pushTasks = IntStream.range(0, THREADS_NUMBER)
                .mapToObj(threadI -> (Runnable) () ->
                        IntStream.range(0, TASKS_NUMBER)
                                .forEach(taskI -> {
                                    try {
                                        // start simultaneously every iteration
                                        barrier.await(1, SECONDS);
                                        Thread.sleep((long) (Math.random() * 10));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    Thread.yield();
                                    cache.put(taskI, threadI * 10 + taskI);
                                })
                )
                .map(Executors::callable)
                .collect(toList());
        List<Future<Object>> futures = executorService.invokeAll(pushTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures) {
            future.get();
        }
        IntStream.range(0, TASKS_NUMBER)
            .forEach(i -> assertTrue(cache.get(i).isPresent()));
        IntStream.range(0, TASKS_NUMBER)
            .forEach(i -> assertTrue(cache.evict().isPresent()));
        assertFalse(cache.evict().isPresent());
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    public void evict() throws InterruptedException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER);
        IntStream.range(0, THREADS_NUMBER).forEach(i -> cache.put(i, i));

        List<Callable<Object>> evictTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    // start simultaneously every iteration
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                cache.evict();
            })
            .map(Executors::callable)
            .collect(toList());
        List<Future<Object>> futures = executorService.invokeAll(evictTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures) {
            future.get();
        }
        assertFalse(cache.evict().isPresent());
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    public void remove() throws InterruptedException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER);
        IntStream.range(0, THREADS_NUMBER).forEach(i -> cache.put(i, i));

        List<Callable<Object>> removeTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    // start simultaneously every iteration
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                cache.remove(threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        List<Future<Object>> futures = executorService.invokeAll(removeTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures) {
            future.get();
        }
        assertFalse(cache.evict().isPresent());
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    public void get() throws InterruptedException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER);
        IntStream.range(0, THREADS_NUMBER).forEach(i -> cache.put(i, i));

        List<Callable<Integer>> getTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Integer>) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return cache.get(threadI).orElseThrow(IllegalStateException::new);
            })
            .collect(toList());

        List<Future<Integer>> futures = executorService.invokeAll(getTasks);

        for (int i = 0; i < THREADS_NUMBER; i++) {
            assertThat(futures.get(i).get(), is(i));
        }
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    public void evictPut() throws InterruptedException, ExecutionException {
        int capacity = TASKS_NUMBER * THREADS_NUMBER * 2;
        Cache<Integer, Integer> cache = getCache(capacity);
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER * 2);

        List<Callable<Object>> putEvictTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                IntStream.range(0, TASKS_NUMBER)
                    .forEach(taskI -> {
                        Thread.yield();
                        cache.put(threadI * 10 + taskI, threadI * 10 + taskI);
                    });
            })
            .map(Executors::callable)
            .collect(toList());

        Map.Entry<List<Callable<Object>>, Supplier<Integer>> evictTasksToMisses = buildEvictTasks(cache, barrier);

        putEvictTasks.addAll(evictTasksToMisses.getKey());
        Collections.shuffle(putEvictTasks);
        List<Future<Object>> futures = executorService.invokeAll(putEvictTasks);

        for (Future<Object> future : futures) {
            future.get();
        }
        assertThat(cache.getSize(), is(evictTasksToMisses.getValue().get()));
        for (int i = 0; i < evictTasksToMisses.getValue().get(); i++) {
            assertThat(cache.evict(), not(Optional.empty()));
        }
        assertThat(cache.evict(), is(Optional.empty()));
    }

    @Test(timeout = 10_000)
    public void evictRemove() throws InterruptedException, ExecutionException {
        int capacity = TASKS_NUMBER * THREADS_NUMBER * 2;
        Cache<Integer, Integer> cache = getCache(capacity);
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER * 2);
        IntStream.range(0, THREADS_NUMBER * TASKS_NUMBER * 2).forEach(i -> cache.put(i, i));

        Map.Entry<List<Callable<Object>>, Supplier<Integer>> removeTasks = buildRemoveTasks(cache, barrier);
        List<Callable<Object>> removeEvictTasks = removeTasks.getKey();

        Map.Entry<List<Callable<Object>>, Supplier<Integer>> evictTasksToMisses = buildEvictTasks(cache, barrier);

        removeEvictTasks.addAll(evictTasksToMisses.getKey());
        Collections.shuffle(removeEvictTasks);
        List<Future<Object>> futures = executorService.invokeAll(removeEvictTasks);

        for (Future<Object> future : futures) {
            future.get();
        }
        assertThat(cache.getSize(), is(evictTasksToMisses.getValue().get() + removeTasks.getValue().get()));
        for (int i = 0; i < evictTasksToMisses.getValue().get() + removeTasks.getValue().get(); i++) {
            assertThat(cache.evict(), not(Optional.empty()));
        }
        assertThat(cache.evict(), is(Optional.empty()));
    }

    @Test(timeout = 10_000)
    public void evictPutRemove() throws InterruptedException, ExecutionException {
        int capacity = TASKS_NUMBER * THREADS_NUMBER * 2;
        Cache<Integer, Integer> cache = getCache(capacity);
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER * 2);

        Map.Entry<List<Callable<Object>>, Supplier<Integer>> removeTasksToMisses = buildRemoveTasks(cache, barrier);
        List<Callable<Object>> removeEvictPutTasks = removeTasksToMisses.getKey();

        Map.Entry<List<Callable<Object>>, Supplier<Integer>> evictTasksToMisses = buildEvictTasks(cache, barrier);

        List<Callable<Object>> putTasks = IntStream.range(0, THREADS_NUMBER * 2)
            .mapToObj(threadI -> (Runnable) () -> {
                try {
                    barrier.await(1, SECONDS);
                    Thread.sleep((long) (Math.random() * 10));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                IntStream.range(0, TASKS_NUMBER)
                    .forEach(taskI -> {
                        Thread.yield();
                        cache.put(threadI * 10 + taskI, threadI * 10 + taskI);
                    });
            })
            .map(Executors::callable)
            .collect(toList());

        removeEvictPutTasks.addAll(evictTasksToMisses.getKey());
        removeEvictPutTasks.addAll(putTasks);
        Collections.shuffle(removeEvictPutTasks);
        List<Future<Object>> futures = executorService.invokeAll(removeEvictPutTasks);

        for (Future<Object> future : futures) {
            future.get();
        }
        assertThat(cache.getSize(), is(evictTasksToMisses.getValue().get() + removeTasksToMisses.getValue().get()));
        for (int i = 0; i < evictTasksToMisses.getValue().get() + removeTasksToMisses.getValue().get(); i++) {
            assertThat(cache.evict(), not(Optional.empty()));
        }
        assertThat(cache.evict(), is(Optional.empty()));
    }

}

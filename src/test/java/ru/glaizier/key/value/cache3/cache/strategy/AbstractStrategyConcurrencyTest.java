package ru.glaizier.key.value.cache3.cache.strategy;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author GlaIZier
 */
public abstract class AbstractStrategyConcurrencyTest {

    private static final int THREADS_NUMBER = 10;

    private static final int TASKS_NUMBER = 10;

    private static ExecutorService executorService;

    private Strategy<Integer> strategy;

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

    static List<Callable<Object>> buildEvictTasks(Strategy<?> strategy, int tasksNumber, CountDownLatch latch) {
        return IntStream.range(0, tasksNumber)
            .mapToObj(threadI -> (Callable<Object>) () -> {
                latch.countDown();
                try {
                    // start simultaneously with all use() and evict()
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return strategy.evict();
            })
            .collect(toList());
    }


    static List<Callable<Object>> buildRemoveTasks(Strategy<? super Integer> strategy, int tasksNumber, CountDownLatch latch) {
        return IntStream.range(0, tasksNumber)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                strategy.remove(threadI);
            })
            .map(Executors::callable)
            .collect(toList());
    }

    static List<Callable<Object>> buildUseTasks(Strategy<? super Integer> strategy, int tasksNumber, CountDownLatch latch) {
        return IntStream.range(0, tasksNumber)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                strategy.use(threadI);
            })
            .map(Executors::callable)
            .collect(toList());
    }

    protected abstract Strategy<Integer> getStrategy();

    @Before
    public void init() {
        strategy = getStrategy();
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    // put every element with taskI simultaneously using CyclicBarrier
    public void use() throws InterruptedException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(THREADS_NUMBER);
        List<Callable<Object>> useTasks = IntStream.range(0, THREADS_NUMBER)
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
                        strategy.use(taskI);
                    })
            )
            .map(Executors::callable)
            .collect(toList());
        List<Future<Object>> futures = executorService.invokeAll(useTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures) {
            future.get();
        }
        IntStream.range(0, TASKS_NUMBER)
            .forEach(i -> assertTrue(strategy.evict().isPresent()));
        assertFalse(strategy.evict().isPresent());
    }

    @Test(timeout = 10_000)
    // get() and put() simultaneously using latch
    public void evictUse() throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 2);
        List<Callable<Object>> evictPushTasks = buildEvictTasks(strategy, THREADS_NUMBER, latch);
        List<Callable<Object>> useTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    // start simultaneously with all put() and get()
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                strategy.use(threadI);
                strategy.use(threadI + TASKS_NUMBER);
            })
            .map(Executors::callable)
            .collect(toList());
        evictPushTasks.addAll(useTasks);
        List<Future<Object>> futures = executorService.invokeAll(evictPushTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures) {
            future.get();
        }
        IntStream.range(0, TASKS_NUMBER)
            .forEach(i -> assertTrue(strategy.evict().isPresent()));
    }

    @Test(timeout = 10_000)
    public void evictRemove() throws InterruptedException, ExecutionException {
        for (int i = 0; i < THREADS_NUMBER; i++) {
            strategy.use(i);
        }

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 2);
        List<Callable<Object>> evictRemoveTasks = buildEvictTasks(strategy, THREADS_NUMBER, latch);
        List<Callable<Object>> removeTasks = buildRemoveTasks(strategy, THREADS_NUMBER, latch);
        evictRemoveTasks.addAll(removeTasks);
        List<Future<Object>> futures = executorService.invokeAll(evictRemoveTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures) {
            future.get();
        }
        assertFalse(strategy.evict().isPresent());
    }

    @Test(timeout = 10_000)
    public void evictUseRemove() throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 5);
        List<Callable<Object>> evictPutRemoveTasks = buildEvictTasks(strategy, THREADS_NUMBER, latch);
        List<Callable<Object>> useTasks = buildUseTasks(strategy, THREADS_NUMBER * 3,  latch);
        List<Callable<Object>> removeTasks = buildRemoveTasks(strategy, THREADS_NUMBER, latch);
        evictPutRemoveTasks.addAll(useTasks);
        evictPutRemoveTasks.addAll(removeTasks);
        List<Future<Object>> futures = executorService.invokeAll(evictPutRemoveTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures) {
            future.get();
        }
        // at least TASKS_NUMBER elements should be present
        IntStream.range(0, TASKS_NUMBER)
            .forEach(i -> assertTrue(strategy.evict().isPresent()));
    }

}

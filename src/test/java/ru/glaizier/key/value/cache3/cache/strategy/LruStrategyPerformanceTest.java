package ru.glaizier.key.value.cache3.cache.strategy;

import static java.util.concurrent.TimeUnit.SECONDS;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.glaizier.key.value.cache3.cache.strategy.AbstractStrategyConcurrencyTest.buildEvictTasks;
import static ru.glaizier.key.value.cache3.cache.strategy.AbstractStrategyConcurrencyTest.buildRemoveTasks;
import static ru.glaizier.key.value.cache3.cache.strategy.AbstractStrategyConcurrencyTest.buildUseTasks;

/**
 * @author GlaIZier
 */
public class LruStrategyPerformanceTest {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int THREADS_NUMBER = 100;

    private static final int TASKS_NUMBER = 10000;

    private static ExecutorService executorService;

    @BeforeClass
    public static void init() {
        executorService = Executors.newFixedThreadPool(THREADS_NUMBER);
    }

    @AfterClass
    public static void cleanUpClass() throws InterruptedException {
        executorService.shutdownNow();
        if (!executorService.awaitTermination(1, SECONDS))
            System.exit(0);
    }

    @Test
    public void concurrentLruIsEqualToSynchronous() throws InterruptedException {
        Strategy<Integer> strategy = new SynchronizedStrategy<>(new LruStrategy<>());
        // don't use latch for these tests
        CountDownLatch countDownLatch = new CountDownLatch(0);
        List<Callable<Object>> evictUseRemoveTasks = buildEvictTasks(strategy, TASKS_NUMBER, countDownLatch);
        List<Callable<Object>> useTasks = buildUseTasks(strategy, TASKS_NUMBER, countDownLatch);
        List<Callable<Object>> removeTasks = buildRemoveTasks(strategy, TASKS_NUMBER, countDownLatch);
        evictUseRemoveTasks.addAll(useTasks);
        evictUseRemoveTasks.addAll(removeTasks);
        Collections.shuffle(evictUseRemoveTasks);

        long start = System.currentTimeMillis();
        executorService.invokeAll(evictUseRemoveTasks);
        // choose randomly a task and print it to disable optimization
        strategy.evict();
        long synchronousStrategyDuration = System.currentTimeMillis() - start;
        log.info("SynchronousLruStrategy's duration: {} ms", synchronousStrategyDuration);

        strategy = new ConcurrentLruStrategy<>();
        evictUseRemoveTasks = buildEvictTasks(strategy, TASKS_NUMBER, countDownLatch);
        useTasks = buildUseTasks(strategy, TASKS_NUMBER, countDownLatch);
        removeTasks = buildRemoveTasks(strategy, TASKS_NUMBER, countDownLatch);
        evictUseRemoveTasks.addAll(useTasks);
        evictUseRemoveTasks.addAll(removeTasks);
        Collections.shuffle(evictUseRemoveTasks);

        start = System.currentTimeMillis();
        executorService.invokeAll(evictUseRemoveTasks);
        // choose randomly a task and print it to disable optimization
        strategy.evict();
        long concurrentStrategyDuration = System.currentTimeMillis() - start;
        log.info("ConcurrentLruStrategy's duration: {} ms", concurrentStrategyDuration);

        // just assert that
        assertThat((double) concurrentStrategyDuration, is(lessThan(synchronousStrategyDuration * 1.1)));
    }

}

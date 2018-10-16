package ru.glaizier.key.value.cache3.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.*;
import static java.util.stream.Collectors.toList;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.describedAs;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;

import static ru.glaizier.key.value.cache3.util.function.Functions.wrap;

/**
 * @author GlaIZier
 */
public class FileStorageConcurrencyTest {

    private static final int THREADS_NUMBER = 10;

    private static final int TASKS_NUMBER = 10;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    private Storage<Integer, String> storage;
    private Storage<HashCodeEqualsPojo, String> collisionsStorage;

    private static class HashCodeEqualsPojo implements Serializable {
        private final int i;
        private final String s;

        private HashCodeEqualsPojo(int i, String s) {
            Objects.requireNonNull(s, "s");
            this.i = i;
            this.s = s;
        }

        int getI() {
            return i;
        }

        String getS() {
            return s;
        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public boolean equals(Object that) {
            if (super.equals(that))
                return true;
            if (!(that instanceof HashCodeEqualsPojo))
                return false;
            HashCodeEqualsPojo castedThat = (HashCodeEqualsPojo) that;
            return (castedThat.getI() == this.getI()) && castedThat.getS().equals(this.getS());
        }
    }

    @Before
    public void init() {
        storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
        collisionsStorage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    // put every element with taskI simultaneously using CyclicBarrier
    public void put() throws InterruptedException, ExecutionException {
        Storage<Integer, String> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());

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
                            storage.put(taskI, format("Thread: %d. Task: %d", threadI, taskI));
                        })
                )
                .map(Executors::callable)
                .collect(toList());
        List<Future<Object>> futures = executorService.invokeAll(pushTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures)
            future.get();
        assertThat(storage.getSize(), is(THREADS_NUMBER));
        IntStream.range(0, TASKS_NUMBER)
            .forEach(taskI -> assertThat("taskI: " + taskI, storage.contains(taskI), is(true)));
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    // get() and put() simultaneously using latch
    public void getPut() throws InterruptedException, ExecutionException {
        Storage<Integer, Integer> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
        storage.put(0, -1);

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 2);
        List<Callable<Object>> getPushTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Object>) () -> {
                latch.countDown();
                try {
                    // start simultaneously with all put() and get()
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return storage.get(0).orElseThrow(IllegalStateException::new);
            })
            .collect(toList());
        List<Callable<Object>> pushTasks = IntStream.range(0, THREADS_NUMBER)
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
                storage.put(0, threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        getPushTasks.addAll(pushTasks);
        List<Future<Object>> futures = executorService.invokeAll(getPushTasks);

        // check that there were no exceptions in futures
        for (Future<Object> future : futures)
            future.get();
        assertThat(storage.getSize(), is(1));
        // First THREAD_NUMBER elements are Integers
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++) {
            assertThat("threadI: " + threadI, (Integer) futures.get(threadI).get(),
                allOf(greaterThanOrEqualTo(-1), lessThan(THREADS_NUMBER)));
        }
    }

    @Test
    public void remove() {
        storage.put(1, "1");
        storage.put(2, "2");

        assertThat(storage.remove(1), is(Optional.of("1")));
        assertThat(storage.get(1), is(Optional.empty()));
        assertThat(storage.getSize(), is(1));
        assertFalse(storage.contains(1));

        assertThat(storage.remove(2), is(Optional.of("2")));
        assertThat(storage.get(2), is(Optional.empty()));
        assertThat(storage.getSize(), is(0));
        assertFalse(storage.contains(2));

        assertThat(storage.remove(2), is(Optional.empty()));
        assertThat(storage.get(2), is(Optional.empty()));
        assertThat(storage.getSize(), is(0));
        assertFalse(storage.contains(2));
    }

    @Test
    public void putWithCollisions() {
        HashCodeEqualsPojo key10 = new HashCodeEqualsPojo(1, "0");
        HashCodeEqualsPojo key11 = new HashCodeEqualsPojo(1, "1");
        HashCodeEqualsPojo key20 = new HashCodeEqualsPojo(2, "0");
        collisionsStorage.put(key10, "10");
        collisionsStorage.put(key11, "11");
        collisionsStorage.put(key20, "20");

        assertThat(collisionsStorage.getSize(), is(3));
        assertTrue(collisionsStorage.contains(key10));
        assertTrue(collisionsStorage.contains(key11));
        assertTrue(collisionsStorage.contains(key20));

        // put with the same key
        collisionsStorage.put(key20, "21");
        assertThat(collisionsStorage.getSize(), is(3));
        assertTrue(collisionsStorage.contains(key10));
        assertTrue(collisionsStorage.contains(key11));
        assertTrue(collisionsStorage.contains(key20));
    }

    @Test
    public void getWithCollisions() {
        HashCodeEqualsPojo key10 = new HashCodeEqualsPojo(1, "0");
        HashCodeEqualsPojo key11 = new HashCodeEqualsPojo(1, "1");
        HashCodeEqualsPojo key12 = new HashCodeEqualsPojo(1, "2");
        HashCodeEqualsPojo key20 = new HashCodeEqualsPojo(2, "0");
        collisionsStorage.put(key10, "10");
        collisionsStorage.put(key11, "11");
        collisionsStorage.put(key12, "11");
        collisionsStorage.put(key20, "20");

        assertThat(collisionsStorage.get(key10), is(Optional.of("10")));
        assertThat(collisionsStorage.get(key11), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key12), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key20), is(Optional.of("20")));

        // put with the same key
        collisionsStorage.put(key20, "21");
        collisionsStorage.put(key11, "13");
        assertThat(collisionsStorage.get(key20), is(Optional.of("21")));
        assertThat(collisionsStorage.get(key11), is(Optional.of("13")));
    }

    @Test
    public void removeWithCollisions() {
        HashCodeEqualsPojo key10 = new HashCodeEqualsPojo(1, "0");
        HashCodeEqualsPojo key11 = new HashCodeEqualsPojo(1, "1");
        HashCodeEqualsPojo key12 = new HashCodeEqualsPojo(1, "2");
        HashCodeEqualsPojo key20 = new HashCodeEqualsPojo(2, "0");
        collisionsStorage.put(key10, "10");
        collisionsStorage.put(key11, "11");
        collisionsStorage.put(key12, "11");
        collisionsStorage.put(key20, "20");

        assertThat(collisionsStorage.remove(key10), is(Optional.of("10")));
        assertThat(collisionsStorage.get(key10), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(3));
        assertFalse(collisionsStorage.contains(key10));

        assertThat(collisionsStorage.remove(key11), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key11), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(2));
        assertFalse(collisionsStorage.contains(key11));

        assertThat(collisionsStorage.remove(key12), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key12), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(1));
        assertFalse(collisionsStorage.contains(key12));

        assertThat(collisionsStorage.remove(key20), is(Optional.of("20")));
        assertThat(collisionsStorage.get(key20), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(0));
        assertFalse(collisionsStorage.contains(key20));

        assertThat(collisionsStorage.remove(key20), is(Optional.empty()));
        assertThat(collisionsStorage.get(key20), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(0));
        assertFalse(collisionsStorage.contains(key20));
    }

    @Test
    public void buildContents() {
        // Files that don't stick to FileName pattern or can't be deserialized won't appear in contents
        IntStream.rangeClosed(1, 2).forEach(i -> {
            try {
                temporaryFolder.newFile(UUID.randomUUID().toString() + ".ser");
                temporaryFolder.newFile(i + "#" + UUID.randomUUID().toString() + ".ser");
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });

        Storage<Integer, String> localStorage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());

        assertTrue(localStorage.isEmpty());
    }

}

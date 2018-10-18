package ru.glaizier.key.value.cache3.storage;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author GlaIZier
 */
public class FileStorageConcurrencyTest {

    private static final int THREADS_NUMBER = 10;

    private static final int TASKS_NUMBER = 10;

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static class HashCodeEqualsPojo implements Serializable {
        private final int hashCodeNumber;
        private final int equalsNumber;

        private HashCodeEqualsPojo(int hashCodeNumber, int equalsNumber) {
            this.hashCodeNumber = hashCodeNumber;
            this.equalsNumber = equalsNumber;
        }

        int getHashCodeNumber() {
            return hashCodeNumber;
        }

        int getEqualsNumber() {
            return equalsNumber;
        }

        @Override
        public int hashCode() {
            return hashCodeNumber;
        }

        @Override
        public boolean equals(Object that) {
            if (super.equals(that))
                return true;
            if (!(that instanceof HashCodeEqualsPojo))
                return false;
            HashCodeEqualsPojo castedThat = (HashCodeEqualsPojo) that;
            return (castedThat.getHashCodeNumber() == this.getHashCodeNumber()) && castedThat.getEqualsNumber() == this.getEqualsNumber();
        }
    }

    @AfterClass
    public static void cleanUpClass() throws InterruptedException {
        executorService.shutdownNow();
        if (!executorService.awaitTermination(1, SECONDS))
            System.exit(0);
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
    // get() and put() simultaneously using latch
    public void getPut() throws InterruptedException, ExecutionException {
        Storage<Integer, Integer> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
        storage.put(0, -1);

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 2);
        List<Callable<Object>> getPutTasks = IntStream.range(0, THREADS_NUMBER)
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
        getPutTasks.addAll(pushTasks);
        List<Future<Object>> futures = executorService.invokeAll(getPutTasks);

        assertThat(storage.getSize(), is(1));
        // First THREAD_NUMBER elements are Integers
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++) {
            assertThat("threadI: " + threadI, (Integer) futures.get(threadI).get(),
                allOf(greaterThanOrEqualTo(-1), lessThan(THREADS_NUMBER)));
        }
    }

    @Test(timeout = 10_000)
    // get() and remove() simultaneously using latch
    public void getRemove() throws InterruptedException, ExecutionException {
        Storage<Integer, Integer> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
        for (int i = 0; i < THREADS_NUMBER; i++) {
            storage.put(i, i);
        }

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 2);
        List<Callable<Object>> getRemoveTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Object>) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return storage.get(threadI);
            })
            .collect(toList());
        List<Callable<Object>> removeTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                storage.remove(threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        getRemoveTasks.addAll(removeTasks);
        List<Future<Object>> futures = executorService.invokeAll(getRemoveTasks);

        assertThat(storage.isEmpty(), is(true));
        // First THREAD_NUMBER elements are Optional<Integer>
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++) {
            assertThat("threadI: " + threadI, futures.get(threadI).get(),
                anyOf(is(Optional.of(threadI)), is(Optional.empty())));
        }
    }

    @Test(timeout = 10_000)
    // get(), put and remove() simultaneously using latch
    public void getPutRemove() throws InterruptedException, ExecutionException {
        Storage<Integer, Integer> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 3);
        List<Callable<Object>> getPutRemoveTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Object>) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return storage.get(threadI);
            })
            .collect(toList());
        List<Callable<Object>> putTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                storage.put(threadI, threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        List<Callable<Object>> removeTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                storage.remove(threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        getPutRemoveTasks.addAll(putTasks);
        getPutRemoveTasks.addAll(removeTasks);
        List<Future<Object>> futures = executorService.invokeAll(getPutRemoveTasks);

        // check that values are either absent, either equal to threadI
        assertThat(storage.getSize(), allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(THREADS_NUMBER)));
        // First THREAD_NUMBER elements are Optional<Integer>
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++)
            assertThat("threadI: " + threadI, futures.get(threadI).get(),
                anyOf(is(Optional.of(threadI)), is(Optional.empty())));

        List<Callable<Optional<Integer>>> getTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Optional<Integer>>) () -> {
                try {
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return storage.get(threadI);
            })
            .collect(toList());
        List<Future<Optional<Integer>>> getFutures = executorService.invokeAll(getTasks);
        AtomicInteger countPresent = new AtomicInteger();
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++)
            getFutures.get(threadI).get().ifPresent(v -> countPresent.incrementAndGet());
        assertThat(storage.getSize(), is(countPresent.get()));
    }

    @Test(timeout = 10_000)
    // timeout in case of deadlocks
    // put every element with taskI simultaneously using CyclicBarrier
    public void putWithCollisions() throws InterruptedException, ExecutionException {
        Storage<HashCodeEqualsPojo, String> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());

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
                        storage.put(new HashCodeEqualsPojo(0, taskI), format("Thread: %d. Task: %d", threadI, taskI));
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
            .forEach(taskI -> assertThat("taskI: " + taskI, storage.contains(new HashCodeEqualsPojo(0, taskI)), is(true)));
    }

    @Test(timeout = 10_000)
    // get() and put() simultaneously using latch
    public void getPutWithCollisions() throws InterruptedException, ExecutionException {
        Storage<HashCodeEqualsPojo, Integer> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
        storage.put(new HashCodeEqualsPojo(0, 0), -1);

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 2);
        List<Callable<Object>> getPutTasks = IntStream.range(0, THREADS_NUMBER)
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
                return storage.get(new HashCodeEqualsPojo(0, 0)).orElseThrow(IllegalStateException::new);
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
                storage.put(new HashCodeEqualsPojo(0, 0), threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        getPutTasks.addAll(pushTasks);
        List<Future<Object>> futures = executorService.invokeAll(getPutTasks);

        assertThat(storage.getSize(), is(1));
        // First THREAD_NUMBER elements are Integers
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++) {
            assertThat("threadI: " + threadI, (Integer) futures.get(threadI).get(),
                allOf(greaterThanOrEqualTo(-1), lessThan(THREADS_NUMBER)));
        }
    }

    @Test(timeout = 10_000)
    // get() and remove() simultaneously using latch
    public void getRemoveWithCollisions() throws InterruptedException, ExecutionException {
        Storage<HashCodeEqualsPojo, Integer> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
        for (int i = 0; i < THREADS_NUMBER; i++) {
            storage.put(new HashCodeEqualsPojo(0, i), i);
        }

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 2);
        List<Callable<Object>> getRemoveTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Object>) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return storage.get(new HashCodeEqualsPojo(0, threadI));
            })
            .collect(toList());
        List<Callable<Object>> removeTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                storage.remove(new HashCodeEqualsPojo(0, threadI));
            })
            .map(Executors::callable)
            .collect(toList());
        getRemoveTasks.addAll(removeTasks);
        List<Future<Object>> futures = executorService.invokeAll(getRemoveTasks);

        assertThat(storage.isEmpty(), is(true));
        // First THREAD_NUMBER elements are Optional<Integer>
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++) {
            assertThat("threadI: " + threadI, futures.get(threadI).get(),
                anyOf(is(Optional.of(threadI)), is(Optional.empty())));
        }
    }

    @Test(timeout = 10_000)
    // get(), put and remove() simultaneously using latch
    public void getPutRemoveWithCollisions() throws InterruptedException, ExecutionException {
        Storage<HashCodeEqualsPojo, Integer> storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());

        CountDownLatch latch = new CountDownLatch(THREADS_NUMBER * 3);
        List<Callable<Object>> getPutRemoveTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Object>) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return storage.get(new HashCodeEqualsPojo(0, threadI));
            })
            .collect(toList());
        List<Callable<Object>> putTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                storage.put(new HashCodeEqualsPojo(0, threadI), threadI);
            })
            .map(Executors::callable)
            .collect(toList());
        List<Callable<Object>> removeTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Runnable) () -> {
                latch.countDown();
                try {
                    latch.await();
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                storage.remove(new HashCodeEqualsPojo(0, threadI));
            })
            .map(Executors::callable)
            .collect(toList());
        getPutRemoveTasks.addAll(putTasks);
        getPutRemoveTasks.addAll(removeTasks);
        List<Future<Object>> futures = executorService.invokeAll(getPutRemoveTasks);

        // check that values are either absent, either equal to threadI
        assertThat(storage.getSize(), allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(THREADS_NUMBER)));
        // First THREAD_NUMBER elements are Optional<Integer>
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++)
            assertThat("threadI: " + threadI, futures.get(threadI).get(),
                anyOf(is(Optional.of(threadI)), is(Optional.empty())));

        List<Callable<Optional<Integer>>> getTasks = IntStream.range(0, THREADS_NUMBER)
            .mapToObj(threadI -> (Callable<Optional<Integer>>) () -> {
                try {
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Thread.yield();
                return storage.get(new HashCodeEqualsPojo(0, threadI));
            })
            .collect(toList());
        List<Future<Optional<Integer>>> getFutures = executorService.invokeAll(getTasks);
        AtomicInteger countPresent = new AtomicInteger();
        for (int threadI = 0; threadI < THREADS_NUMBER; threadI++)
            getFutures.get(threadI).get().ifPresent(v -> countPresent.incrementAndGet());
        assertThat(storage.getSize(), is(countPresent.get()));
    }
}

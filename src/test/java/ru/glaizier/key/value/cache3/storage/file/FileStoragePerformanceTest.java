package ru.glaizier.key.value.cache3.storage.file;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.glaizier.key.value.cache3.storage.Storage;
import ru.glaizier.key.value.cache3.storage.SynchronousStorage;

/**
 * @author GlaIZier
 */
public class FileStoragePerformanceTest {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int THREADS_NUMBER = 10;

    private static final int TASKS_NUMBER = 100;

    private static ExecutorService executorService;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Storage<Integer, Integer> storage;

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
    public void concurrentStorageEfficiency() throws IOException, InterruptedException, ExecutionException {
        List<Callable<Object>> pushTasks = buildPushTasks(1, TASKS_NUMBER + 1, TASKS_NUMBER);

        storage = new SynchronousStorage<>(new FileStorage<>(temporaryFolder.newFolder().toPath()));
        long start = System.currentTimeMillis();
        List<Future<Object>> tasks = executorService.invokeAll(pushTasks);
        // choose randomly a task and print it to disable optimization
        int numberToGet = new Random().nextInt(TASKS_NUMBER) + 1;
        log.trace((String.valueOf(storage.getSize())));
        log.trace(storage.get(numberToGet).toString());
        System.out.println(System.currentTimeMillis() - start);
    }

    private List<Callable<Object>> buildPushTasks(int leftBoundIncl, int rightBoundExcl, int tasksNum) {
        return IntStream.range(0, tasksNum)
            .mapToObj(outerTaskId -> (Runnable) () -> {
                    List<Integer> randomNums = new Random().ints(tasksNum, leftBoundIncl, rightBoundExcl).boxed()
                        .collect(toList());
                    IntStream.range(0, tasksNum)
                        .forEach(innerTaskId -> storage.put(randomNums.get(innerTaskId), randomNums.get(innerTaskId)));
                }
            )
            .map(Executors::callable)
            .collect(toList());
    }

}

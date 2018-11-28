package ru.glaizier.key.value.cache3.storage.file;

import static java.util.concurrent.TimeUnit.SECONDS;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import ru.glaizier.key.value.cache3.storage.Storage;
import ru.glaizier.key.value.cache3.storage.SynchronousStorage;

/**
 * @author GlaIZier
 */
public class FileStoragePerformanceTest {
    private static final int THREADS_NUMBER = 10;

    private static final int TASKS_NUMBER = 100;

    private static ExecutorService executorService;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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
    public void concurrentStorageEfficiency() throws IOException {
        Storage storage = new SynchronousStorage<Integer, Integer>(new FileStorage<>(temporaryFolder.newFolder().toPath()));

    }

}

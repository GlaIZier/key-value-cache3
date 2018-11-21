package ru.glaizier.key.value.cache3.storage.file;

import static java.util.Optional.ofNullable;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import ru.glaizier.key.value.cache3.storage.StorageException;

/**
 * Writes on disk are confined to one thread
 */
@ThreadSafe
public class ConfinedFileStorage<K extends Serializable, V extends Serializable> extends AbstractFileStorage<K, V> {

    // Guards the folder and the contents-disk invariant
    private final ExecutorService diskWorker = Executors.newSingleThreadExecutor();

    public ConfinedFileStorage() {
        this(TEMP_FOLDER);
    }

    public ConfinedFileStorage(@Nonnull Path folder) {
        super(folder);
    }

    @Override
    public Optional<V> get(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        Future<Optional<V>> task = diskWorker.submit(() -> ofNullable(contents.get(key))
            .map(path -> deserialize(path).value));
        return getFutureValue(task);
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        Future<Optional<V>> task = diskWorker.submit(() -> {
            Optional<Path> prevPathOpt = ofNullable(contents.get(key));
            Optional<V> prevValueOpt = prevPathOpt
                .map(prevPath -> deserialize(prevPath).value);
            Path path = serialize(key, value);
            contents.put(key, path);
            prevPathOpt.ifPresent(this::removeFile);
            return prevValueOpt;
        });
        return getFutureValue(task);
    }

    @Override
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        Future<Optional<V>> task = diskWorker.submit(() -> {
            Optional<Path> removedPathOpt = ofNullable(contents.remove(key));
            Optional<V> prevValue = removedPathOpt.map(prevPath -> deserialize(prevPath).value);
            removedPathOpt.ifPresent(this::removeFile);
            return prevValue;
        });
        return getFutureValue(task);
    }

    public boolean stopDiskWorker() throws InterruptedException {
        // Todo add shutdownNow()?
        diskWorker.shutdown();
        return diskWorker.awaitTermination(10, TimeUnit.SECONDS);
    }

    private Optional<V> getFutureValue(Future<Optional<V>> future) {
        try {
            return future.get();
        } catch (Exception e) {
            if (e.getCause() instanceof StorageException)
                throw (StorageException) e.getCause();
            throw new StorageException(e.getMessage(), e);
        }
    }

}

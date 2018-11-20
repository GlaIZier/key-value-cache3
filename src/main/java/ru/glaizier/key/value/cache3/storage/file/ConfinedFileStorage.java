package ru.glaizier.key.value.cache3.storage.file;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toConcurrentMap;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.glaizier.key.value.cache3.storage.Storage;
import ru.glaizier.key.value.cache3.storage.StorageException;
import ru.glaizier.key.value.cache3.util.Entry;

/**
 * Writes on disk are confined to one thread
 */
@ThreadSafe
public class ConfinedFileStorage<K extends Serializable, V extends Serializable> implements Storage<K, V> {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // filename format: <keyHash>-<uuid>.ser
    private static final String FILENAME_FORMAT = "%d#%s.ser";

    private static final Path TEMP_FOLDER = Paths.get(System.getProperty("java.io.tmpdir")).resolve("key-value-cache3");

    private static final Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)#(\\S+)\\.(ser)$");

    private final ConcurrentMap<K, Path> contents;

    private final ExecutorService diskWorker = Executors.newSingleThreadExecutor();

    @GuardedBy("diskWorker")
    private final Path folder;

    public ConfinedFileStorage() {
        this(TEMP_FOLDER);
    }

    public ConfinedFileStorage(Path folder) {
        Objects.requireNonNull(folder, "folder");
        this.folder = folder;
        try {
            if (Files.notExists(folder)) {
                Files.createDirectories(folder);
            }
            contents = buildContents(folder);
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    private ConcurrentMap<K, Path> buildContents(Path folder) throws IOException {
        return Files.walk(folder)
                .filter(Files::isRegularFile)
                .filter(path -> FILENAME_PATTERN.matcher(path.getFileName().toString()).find())
                .map(path -> {
                    try {
                        return new Entry<>(deserialize(path).key, path);
                    } catch (Exception e) {
                        log.error("Couldn't deserialize key-value for the path: " + path, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toConcurrentMap(entry -> entry.key, entry -> entry.value, (one, another) -> one));
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

    @Override
    public boolean contains(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return contents.containsKey(key);
    }

    @Override
    public int getSize() {
        return contents.size();
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

    private Optional<Entry<Path, V>> find(@Nonnull K key) {
        Future<Optional<Entry<Path, V>>> task = diskWorker.submit(() -> {
            if (contents.containsKey(key)) {
                Path path = contents.get(key);
                return of(new Entry<>(path, deserialize(path).value));
            }
            return empty();
        });
        try {
            return task.get();
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    // Not thread-safe. Call with proper sync if needed
    @SuppressWarnings("unchecked")
    private Entry<K, V> deserialize(@Nonnull Path path) throws StorageException {
        try (FileChannel channel = FileChannel.open(path);
             ObjectInputStream ois = new ObjectInputStream(Channels.newInputStream(channel))) {
            // create a shared lock to let other processes reading too
            FileLock fileLock = channel.lock(0L, Long.MAX_VALUE, true);
            try {
                return (Entry) ois.readObject();
            } finally {
                fileLock.release();
            }
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    // Not thread-safe. Call with proper sync if needed
    private Path serialize(@Nonnull K key, @Nonnull V value) throws StorageException {
        String filename = format(FILENAME_FORMAT, key.hashCode(), UUID.randomUUID().toString());
        Path serialized = folder.resolve(filename);
        boolean fileSaved = false;
        try (FileOutputStream fos = new FileOutputStream(serialized.toFile());
             FileChannel channel = fos.getChannel();
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            // exclusive lock access to the file by OS
            FileLock fileLock = channel.lock();
            try {
                Entry<K, V> entry = new Entry<>(key, value);
                oos.writeObject(entry);
                fileSaved = true;
                return serialized;
            } finally {
                fileLock.release();
            }
        } catch (Exception e) {
            if (fileSaved)
                throw new InconsistentFileStorageException(format("Exception after saving file %s has occurred", serialized), e, serialized);
            else
                throw new StorageException(e.getMessage(), e);
        }
    }

    // Not thread-safe. Call with proper sync if needed
    private boolean removeFile(@Nonnull Path path) throws StorageException {
        try {
            return Files.deleteIfExists(path);
        } catch (Exception e) {
            throw new RedundantFileStorageException("Couldn't remove file", e, path);
        }
    }

}

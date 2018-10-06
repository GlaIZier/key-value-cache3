package ru.glaizier.key.value.cache3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.glaizier.key.value.cache3.util.Entry;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toConcurrentMap;
import static ru.glaizier.key.value.cache3.util.function.Functions.wrap;

// Todo create a single thread executor alternative to deal with io?
// Todo @GuardedBy
@ThreadSafe
// We don't use local locks for locking. Objects in the heap (locks map) are used to introduce flexible locking
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class FileStorageConcurrent<K extends Serializable, V extends Serializable> implements Storage<K, V> {

    // filename format: <keyHash>-<uuid>.ser
    final static String FILENAME_FORMAT = "%d#%s.ser";

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final static Path TEMP_FOLDER = Paths.get(System.getProperty("java.io.tmpdir")).resolve("key-value-cache3");

    private final static Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)#(\\S+)\\.(ser)$");

    private final ConcurrentMap<K, Path> contents;

    // Todo use K as locks?
    private final ConcurrentMap<K, Object> locks;

    private final Path folder;

    public FileStorageConcurrent() {
        this(TEMP_FOLDER);
    }

    public FileStorageConcurrent(Path folder) {
        Objects.requireNonNull(folder, "folder");
        this.folder = folder;
        try {
            if (Files.notExists(folder)) {
                Files.createDirectories(folder);
            }
            contents = buildContents(folder);
            locks = buildLocks(contents.keySet());
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
                .collect(toConcurrentMap(entry -> entry.key, entry -> entry.value));
    }

    private ConcurrentMap<K, Object> buildLocks(Set<K> keys) throws IOException {
        return keys.stream().collect(toConcurrentMap(Function.identity(), v -> new Object()));
    }

    @Override
    public Optional<V> get(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        // double check-lock
        return ofNullable(contents.get(key))
            .flatMap(path -> {
                Object lock = locks.get(key);
                synchronized (lock) {
                    validateInvariant(key);
                    return ofNullable(contents.get(key))
                        .map(lockedPath -> deserialize(lockedPath).value);
                }
            });
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        Object lock = locks.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            Optional<V> prevValueOpt = ofNullable(contents.get(key))
                .map(prevPath -> {
                    V prevValue = deserialize(prevPath).value;
                    wrap(Files::deleteIfExists, StorageException.class).apply(prevPath);
                    return prevValue;
                });
            Path newPath = serialize(key, value);
            contents.put(key, newPath);
            return prevValueOpt;
        }
    }

    @Override
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        // double check-lock
        return ofNullable(contents.get(key))
                .flatMap(path -> {
                    Object lock = locks.get(key);
                    synchronized (lock) {
                        validateInvariant(key);
                        return ofNullable(contents.get(key))
                                .map(lockedPath -> {
                                    V removedValue = deserialize(lockedPath).value;
                                    remove(key, lockedPath);
                                    return removedValue;
                                });
                    }
                });
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

    /**
     * Not thread-safe. Call with proper sync if needed
     * @throws StorageException in all cases including one when a file doesn't exist
     */
    @SuppressWarnings("unchecked")
    private Entry<K, V> deserialize(Path path) throws StorageException {
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


    /**
     * Not thread-safe. Call with proper sync if needed
     */
    private Path serialize(K key, V value) {
        Path serialized = ofNullable(contents.get(key))
            .orElseGet(() -> {
                String filename = format(FILENAME_FORMAT, key.hashCode(), UUID.randomUUID().toString());
                return folder.resolve(filename);
            });
        try (FileOutputStream fos = new FileOutputStream(serialized.toFile());
             FileChannel channel = fos.getChannel();
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            // exclusive lock access to the file by OS
            FileLock fileLock = channel.lock();
            try {
                Entry<K, V> entry = new Entry<>(key, value);
                oos.writeObject(entry);
                return serialized;
            } finally {
                fileLock.release();
            }
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    // Todo remove this invariant checks after testing?
    // Not thread-safe. Call with proper sync if needed
    private void validateInvariant(K key) {
        Path path = contents.get(key);
        Object lock = locks.get(key);
        boolean fileExists = path != null && Files.exists(path);
        if ((path == null && lock == null) ||
            (path != null && lock != null && fileExists))
            return;

        throw new IllegalStateException(format("FileStorage's invariant has been violated: path = %s, lock = %s, fileExists = %b",
            path, lock, fileExists));
    }

    // Not thread-safe. Call with proper sync if needed
    private void remove(K key, Path path) {
        // remove from a disk
        // introduce some repeat logic in case of unsuccessful removal (eg some other process is using the file)?
        // Or rename it randomly before removal to restrict other process to touch it?
        wrap(Files::deleteIfExists, StorageException.class).apply(path);
        // remove from contents and locks
        contents.remove(key);
        // remove from locks
        locks.remove(key);
    }

}

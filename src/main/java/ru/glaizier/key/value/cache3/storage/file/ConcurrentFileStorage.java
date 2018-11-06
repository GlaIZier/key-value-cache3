package ru.glaizier.key.value.cache3.storage.file;

import static java.lang.String.format;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.glaizier.key.value.cache3.storage.Storage;
import ru.glaizier.key.value.cache3.storage.StorageException;
import ru.glaizier.key.value.cache3.util.Entry;
import static ru.glaizier.key.value.cache3.util.function.Functions.wrap;

// Todo create a single thread executor alternative to deal with io?
/**
 * Objects in the heap (locks map) are used to introduce flexible (partial) locking. We could introduce even more
 * flexibility by using ReadWriteLock but it's not worth it for now.
 */
@ThreadSafe
// We don't use local locks for locking (we use locks in the heap)
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class ConcurrentFileStorage<K extends Serializable, V extends Serializable> implements Storage<K, V> {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // filename format: <keyHash>-<uuid>.ser
    private static final String FILENAME_FORMAT = "%d#%s.ser";

    private static final Path TEMP_FOLDER = Paths.get(System.getProperty("java.io.tmpdir")).resolve("key-value-cache3");

    private static final Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)#(\\S+)\\.(ser)$");

    private final ConcurrentMap<K, Path> contents;

    // Todo come up with idea how to remove something from it
    private final ConcurrentMap<K, Object> locks;

    private final Transactional transactional = new Transactional();

    @GuardedBy("locks")
    private final Path folder;

    public ConcurrentFileStorage() {
        this(TEMP_FOLDER);
    }

    public ConcurrentFileStorage(Path folder) {
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
                        return new Entry<>(transactional.deserialize(path).key, path);
                    } catch (Exception e) {
                        log.error("Couldn't deserialize key-value for the path: " + path, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toConcurrentMap(entry -> entry.key, entry -> entry.value, (one, another) -> one));
    }

    private ConcurrentMap<K, Object> buildLocks(Set<K> keys) throws IOException {
        return keys.stream().collect(toConcurrentMap(Function.identity(), v -> new Object()));
    }

    @Override
    public Optional<V> get(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return transactional.get(key);
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return transactional.put(key, value);
    }

    @Override
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return transactional.remove(key);
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
     * Not thread-safe. Call with proper sync.
     * In case of exceptions:
     * If consistency wasn't violated, StorageException is thrown to indicate that you need to try one again.
     * Otherwise, InconsistentFileStorageException is thrown to indicate that you should take care of the inconsistent file
     * manually, call remove() on this key and try again.
     */
    private class Transactional {

        // doesn't change anything. No need to cope with invariants.
        private Optional<V> get(@Nonnull K key) {
            // Todo double-check lock is antipattern?
            // double-check lock
            return ofNullable(locks.get(key))
                .flatMap(lock -> {
                    synchronized (lock) {
                        return ofNullable(contents.get(key))
                            .map(lockedPath -> deserialize(lockedPath).value);
                    }
                });
        }

        // Todo try to rewrite a file with one operation if a path is the same for the same keys
        private Optional<V> put(@Nonnull K key, @Nonnull V value) {
            // Todo fix bug with simultaneous remove: CreateandGet, remove get, removes everything.
            // Todo introduce while logic in every method?
            while (true) {
                Object lock = locks.computeIfAbsent(key, k -> new Object());
                synchronized (lock) {
                    if (lock != locks.get(key))
                        continue;
                    Optional<V> prevValueOpt = ofNullable(contents.get(key))
                        .map(prevPath -> {
                            V prevValue = deserialize(prevPath).value;
                            removeFile(prevPath);
                            return prevValue;
                        });
                    Path newPath = serialize(key, value);
                    try {
                        contents.put(key, newPath);
                    } catch (Exception e) {
                        throw new InconsistentFileStorageException(format("Failed to put path %s for the key %s in the contests", newPath, key),
                            e, newPath);
                    }
                    validateInvariant(key);
                    return prevValueOpt;
                }
            }
        }

        private Optional<V> remove(@Nonnull K key) {
            // double check-lock
            return ofNullable(contents.get(key))
                    .flatMap(path -> {
                        Object lock = locks.get(key);
                        synchronized (lock) {
                            return ofNullable(contents.get(key))
                                    .map(lockedPath -> {
                                        try {
                                            contents.remove(key);
                                        } catch (Exception e) {
                                            throw new StorageException(format("Couldn't remove key %s from the contests", key));
                                        }
                                        V removedValue = deserialize(lockedPath).value;
                                        removeFile(lockedPath);
                                        validateInvariant(key);
                                        return removedValue;
                                    });
                        }
                    });
        }

        // Not thread-safe. Call with proper sync if needed
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

        // Not thread-safe. Call with proper sync if needed
        private Path serialize(K key, V value) throws StorageException, InconsistentFileStorageException {
            Path serialized = ofNullable(contents.get(key))
                    .orElseGet(() -> {
                        String filename = format(FILENAME_FORMAT, key.hashCode(), UUID.randomUUID().toString());
                        return folder.resolve(filename);
                    });
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
                    throw new InconsistentFileStorageException(format("Exception after file save has occurred %s", serialized), e, serialized);
                else
                    throw new StorageException(e.getMessage(), e);
            }
        }

        // Not thread-safe. Call with proper sync if needed
        private void validateInvariant(K key) throws InconsistentFileStorageException {
            Path path = contents.get(key);
            Object lock = locks.get(key);
            boolean fileExists = path != null && Files.exists(path);
            if ((path == null) ||
                    (lock != null && fileExists))
                return;

            throw new InconsistentFileStorageException(format("FileStorage's invariant has been violated: path = %s, lock = %s, fileExists = %b",
                    path, lock, fileExists), path);
        }

        // Not thread-safe. Call with proper sync if needed
        private boolean removeFile(Path path) throws StorageException {
            return wrap(Files::deleteIfExists, StorageException.class).apply(path);
        }

    }

}

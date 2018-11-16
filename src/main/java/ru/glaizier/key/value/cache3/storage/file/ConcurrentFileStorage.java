package ru.glaizier.key.value.cache3.storage.file;

import static java.lang.String.format;
import static java.util.Optional.empty;
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

    private final ConcurrentMap<K, Object> locks;

    private final Transactional transactional = new Transactional();

    @GuardedBy("locks.get()")
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


    // Todo remove it
    private class Transactional {
        // doesn't change anything. No need to cope with invariants.
        private Optional<V> get(@Nonnull K key) throws StorageException {
            while (true) {
                Object lock = locks.get(key);
                if (lock == null)
                    return empty();
                synchronized (lock) {
                    if (lock != locks.get(key))
                        continue;
                    // can be null if the lock was added by push() but not a value
                    return ofNullable(contents.get(key))
                            .map(lockedPath -> deserialize(lockedPath).value);
                }
            }

            // Todo double-check lock is antipattern?
            // double-check lock
            /*
            return ofNullable(locks.get(key))
                .flatMap(lock -> {
                    synchronized (lock) {
                        return ofNullable(contents.get(key))
                            .map(lockedPath -> deserialize(lockedPath).value);
                    }
                });
             */
        }

        private Optional<V> put(@Nonnull K key, @Nonnull V value) throws StorageException {
            Object newLock = new Object();
            while (true) {
                Object lock = locks.computeIfAbsent(key, k -> newLock);
                synchronized (lock) {
                    // if lock changed repeat lock acquiring
                    if (lock != locks.get(key))
                        continue;

                    // save previous state
                    boolean newLockAdded = lock == newLock;
                    boolean saved = false;
                    Optional<Path> prevPathOpt = ofNullable(contents.get(key));

                    try {
                        Optional<V> prevValueOpt = prevPathOpt.map(prevPath -> deserialize(prevPath).value);
                        Path newPath = serialize(key, value);
                        contents.put(key, newPath);
                        saved = true;
                        prevPathOpt.ifPresent(this::removeFile);
                        return prevValueOpt;
                    } catch (StorageException e) {
                        // restore previous state
                        if (!saved) {
                            prevPathOpt.ifPresent(prevPath -> contents.put(key, prevPath));
                            if (newLockAdded)
                                locks.remove(key);
                        }
                        throw e;
                    }
                }
            }
        }

        private Optional<V> remove(@Nonnull K key) throws StorageException, InconsistentFileStorageException {
            while (true) {
                Object lock = locks.get(key);
                if (lock == null)
                    return empty();
                synchronized (lock) {
                    if (lock != locks.get(key))
                        continue;
                    // can be null if lock was added by push() but not the exact value
                    return ofNullable(contents.get(key))
                            .map(lockedPath -> {
                                V removedValue = deserialize(lockedPath).value;
                                contents.remove(key);
                                try {
                                    removeFile(lockedPath);
                                    return removedValue;
                                } catch (InconsistentFileStorageException e) {
                                    throw e;
                                } catch (Exception e) {
                                    throw new InconsistentFileStorageException("Couldn't remove file", e, lockedPath);
                                } finally {
                                    locks.remove(key);
                                }
                            });
                }
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

}

package ru.glaizier.key.value.cache3.storage.file;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toConcurrentMap;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import ru.glaizier.key.value.cache3.storage.StorageException;

/**
 * Objects in the heap (locks map) are used to introduce flexible (partial) locking. We could introduce even more
 * flexibility by using ReadWriteLock but it's not worth it for now.
 */
@ThreadSafe
// We don't use local locks for locking (we use locks in the heap)
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class ConcurrentFileStorage<K extends Serializable, V extends Serializable> extends AbstractFileStorage<K, V> {

    // guards the folder and contents-disk invariant
    private final ConcurrentMap<K, Object> locks;

    public ConcurrentFileStorage() {
        this(TEMP_FOLDER);
    }

    public ConcurrentFileStorage(@Nonnull Path folder) {
        super(folder);
        try {
            locks = buildLocks(contents.keySet());
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    private ConcurrentMap<K, Object> buildLocks(Set<K> keys) throws IOException {
        return keys.stream().collect(toConcurrentMap(Function.identity(), v -> new Object()));
    }

    @Override
    // Todo add while logic to a separate method
    public Optional<V> get(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        // Todo do I need to read it while holding a lock?
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
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

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

    @Override
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

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

}

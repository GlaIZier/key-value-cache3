package ru.glaizier.key.value.cache3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toConcurrentMap;

import ru.glaizier.key.value.cache3.util.Entry;

// Todo create a single thread executor alternative to deal with io?
// Todo @GuardedBy
@ThreadSafe
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class FileStorageConcurrent<K extends Serializable, V extends Serializable> implements Storage<K, V> {

    // filename format: <keyHash>-<uuid>.ser
    final static String FILENAME_FORMAT = "%d#%s.ser";

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final static Path TEMP_FOLDER = Paths.get(System.getProperty("java.io.tmpdir")).resolve("key-value-cache3");

    private final static Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)#(\\S+)\\.(ser)$");

    // Todo do I need locks in a separate map or I can use path in contents as a lock
    // Values are also used as locks
    private final ConcurrentMap<K, Path> contents;

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

    @Override
    public Optional<V> get(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        // Todo double check lock?
        return ofNullable(contents.get(key))
            .map(path -> {
                synchronized (path) {
                    return deserialize(path).value;
                }
            });
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
//        Objects.requireNonNull(key);
//        Objects.requireNonNull(value);
//
//        Object lock = locks.computeIfAbsent(key, k -> new Object());
//
//        Optional<V> prevValue = remove(key);
//
//        putVal(key, value);
//        return prevValue;

        return null;
    }

    @Override
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        return null;
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


    // Not thread-safe. Call with proper sync if needed
    @SuppressWarnings("unchecked")
    private Entry<K, V> deserialize(Path path) {
        try (FileInputStream fis = new FileInputStream(path.toFile())) {
            // lock access to the file by OS
            FileLock fileLock = fis.getChannel().lock();
            try (ObjectInputStream ois = new ObjectInputStream(fis)) {
                return (Entry) ois.readObject();
            } finally {
                fileLock.release();
            }
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    // Not thread-safe. Call with proper sync if needed
    private Path serialize(K key, V value) {
        Path serialized = ofNullable(contents.get(key))
            .orElseGet(() -> {
                String filename = format(FILENAME_FORMAT, key.hashCode(), UUID.randomUUID().toString());
                return folder.resolve(filename);
            });
        Entry<K, V> entry = new Entry<>(key, value);
        try (FileOutputStream fos = new FileOutputStream(serialized.toFile())) {
            // lock access to the file by OS
            FileLock fileLock = fos.getChannel().lock();
            try (ObjectOutputStream oos = new ObjectOutputStream(fos)){
                oos.writeObject(entry);
            } finally {
                fileLock.release();
            }
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
        return serialized;
    }

}

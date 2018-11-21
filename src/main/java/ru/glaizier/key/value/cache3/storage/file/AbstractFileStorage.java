package ru.glaizier.key.value.cache3.storage.file;

import static java.lang.String.format;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.glaizier.key.value.cache3.storage.Storage;
import ru.glaizier.key.value.cache3.storage.StorageException;
import ru.glaizier.key.value.cache3.util.Entry;


@ThreadSafe
public abstract class AbstractFileStorage<K extends Serializable, V extends Serializable> implements Storage<K, V> {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // filename format: <keyHash>-<uuid>.ser
    protected static final String FILENAME_FORMAT = "%d#%s.ser";

    protected static final Path TEMP_FOLDER = Paths.get(System.getProperty("java.io.tmpdir")).resolve("key-value-cache3");

    protected static final Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)#(\\S+)\\.(ser)$");

    protected final ConcurrentMap<K, Path> contents;

    @GuardedBy("derived classes")
    protected final Path folder;

    AbstractFileStorage() {
        this(TEMP_FOLDER);
    }

    AbstractFileStorage(@Nonnull Path folder) {
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

    // call only from a constructor
    protected ConcurrentMap<K, Path> buildContents(Path folder) throws IOException {
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


    // Thread-safe
    @Override
    public boolean contains(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return contents.containsKey(key);
    }

    // Thread-safe
    @Override
    public int getSize() {
        return contents.size();
    }


    // Not thread-safe. Call with proper sync if needed
    // Todo change to tryLock to avoid deadlocks? and add reaction to InterruptedException?
    @SuppressWarnings("unchecked")
    protected Entry<K, V> deserialize(@Nonnull Path path) throws StorageException {
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
    // Todo thread-safe because it uses random path
    protected Path serialize(@Nonnull K key, @Nonnull V value) throws StorageException {
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
    protected boolean removeFile(@Nonnull Path path) throws StorageException {
        try {
            return Files.deleteIfExists(path);
        } catch (Exception e) {
            throw new RedundantFileStorageException("Couldn't remove file", e, path);
        }
    }

}

package ru.glaizier.key.value.cache3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toConcurrentMap;

// Todo create a single thread executor alternative to deal with io?
// Todo add @ThreadSafe and @GuardedBy
@ThreadSafe
public class FileStorageConcurrent<K extends Serializable, V extends Serializable> implements Storage<K, V> {

    // filename format: <keyHash>-<contentsListIndex>.ser
    final static String FILENAME_FORMAT = "%d-%d.ser";

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final static Path TEMP_FOLDER = Paths.get(System.getProperty("java.io.tmpdir")).resolve("key-value-cache3");

    private final static Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)-(\\d+)\\.(ser)$");

    private final ConcurrentMap<K, Path> con;

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
            con = buildContents(folder);
            locks = buildLocks(con.keySet());
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
                        return new SimpleImmutableEntry<>(deserialize(path).getKey(), path);
                    } catch (Exception e) {
                        log.error("Couldn't deserialize key-value for the path: " + path, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toConcurrentMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue));
    }

    private ConcurrentMap<K, Object> buildLocks(Set<K> keys) throws IOException {
        return keys.stream().collect(toConcurrentMap(Function.identity(), v -> new Object()));
    }

    @Override
    public Optional<V> get(@Nonnull K key) {
        return null;
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
        return null;
    }

    @Override
    public Optional<V> remove(@Nonnull K key) {
        return null;
    }

    @Override
    public boolean contains(@Nonnull K key) {
        return false;
    }

    @Override
    public int getSize() {
        return con.size();
    }


    @SuppressWarnings("unchecked")
    private Map.Entry<K, V> deserialize(Path path) {
        try (FileInputStream fis = new FileInputStream(path.toFile());
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            Map.Entry deserialized = (Map.Entry) ois.readObject();
            K key = (K) deserialized.getKey();
            V value = (V) deserialized.getValue();
            return new SimpleImmutableEntry<>(key, value);
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    // Todo
    private Path serialize(K key, V value) {
//        Optional<Path> pathOpt = ofNullable(con.get(key));
//        String fileName = format(FILENAME_FORMAT, key.hashCode(), pathOpt.map(List::size).orElse(0));
//        Path serialized = folder.resolve(fileName);
//        Map.Entry<K, V> entryToSerialize = new SimpleImmutableEntry<>(key, value);
//        try(FileOutputStream fos = new FileOutputStream(serialized.toFile());
//            ObjectOutputStream oos = new ObjectOutputStream(fos)) {
//            oos.writeObject(entryToSerialize);
//            return serialized;
//        } catch (Exception e) {
//            throw new StorageException(e.getMessage(), e);
//        }
        return null;
    }

}

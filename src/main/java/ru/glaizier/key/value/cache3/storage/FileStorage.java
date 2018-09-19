package ru.glaizier.key.value.cache3.storage;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toConcurrentMap;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.glaizier.key.value.cache3.util.function.Functions.wrap;

// Todo create a single thread executor alternative to deal with io?
public class FileStorage<K extends Serializable, V extends Serializable> implements Storage<K, V> {

    // filename format: <keyHash>-<contentsListIndex>.ser
    final static String FILENAME_FORMAT = "%d-%d.ser";

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final static Path TEMP_FOLDER = Paths.get(System.getProperty("java.io.tmpdir")).resolve("key-value-cache3");

    private final static Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)-(\\d+)\\.(ser)$");

    // Hashcode of key to List<Path> on the disk because there can be collisions
    private final Map<Integer, List<Path>> contents;

    private final ConcurrentMap<K, Path> con;

    private final ConcurrentMap<Path, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();

    private final Path folder;

    /**
     * Fully identified element of FileStorage
     */
    private static final class Element<K extends Serializable, V extends Serializable> {
        private final K key;
        private final V value;
        private final Path path;
        private final int contentsListIndex;

        Element(K key, V value, Path path, int contentsListIndex) {
            this.key = key;
            this.value = value;
            this.path = path;
            this.contentsListIndex = contentsListIndex;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public Path getPath() {
            return path;
        }

        public int getContentsListIndex() {
            return contentsListIndex;
        }
    }

    public FileStorage() {
        this(TEMP_FOLDER);
    }

    public FileStorage(Path folder) {
        Objects.requireNonNull(folder, "folder");
        this.folder = folder;
        try {
            if (Files.notExists(folder)) {
                Files.createDirectories(folder);
            }
            contents = createContents(folder);
            con = buildContents(folder);
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

    @Deprecated
    static Map<Integer, List<Path>> createContents(Path folder) throws IOException {
        return Files.walk(folder)
                .filter(Files::isRegularFile)
                .filter(path -> Objects.nonNull(path.getFileName()))
                .filter(path -> {
                    String fileName = path.getFileName().toString();
                    return FILENAME_PATTERN.matcher(fileName).find();
                })
                .collect(Collectors.groupingBy(path -> {
                    String fileName = path.getFileName().toString();
                    Matcher matcher = FILENAME_PATTERN.matcher(fileName);

                    if (matcher.find())
                        return Integer.parseInt(matcher.group(1));
                    else
                        throw new IllegalStateException("Didn't find group in regexp!");
                }));
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

    @Override
    @Deprecated
    public Optional<V> get(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return findElement(key).map(Element::getValue);
    }

    @Override
    @Deprecated
    public Optional<V> put(@Nonnull K key, @Nonnull V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        Optional<V> prevValue = remove(key);

        putVal(key, value);
        return prevValue;
    }

    @Override
    @Deprecated
    public Optional<V> remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return findElement(key)
                .flatMap(this::remove)
                .map(Element::getValue);
    }

    @Override
    @Deprecated
    public boolean contains(@Nonnull K key) {
        Objects.requireNonNull(key, "key");
        return findElement(key).isPresent();
    }

    @Override
    @Deprecated
    public int getSize() {
        return contents.values().stream()
                .mapToInt(List::size)
                .reduce(Integer::sum)
                .orElse(0);
    }

    /**
     * Searches at first for such key the list of paths and then
     * in a list of paths - specific entry using deserialization and keys' equality
     */
    private Optional<? extends Element<K, V>> findElement(K key) {
        Optional<List<Path>> keyPathsOpt = ofNullable(contents.get(key.hashCode()));
        // Use iteration through indexes as we use ArrayList for contents => list.get(index) will work fast
        return keyPathsOpt.flatMap(keyPaths ->
                IntStream.range(0, keyPaths.size())
                        .mapToObj(i -> {
                            Path path = keyPaths.get(i);
                            Map.Entry<K, V> deserialized = deserialize(path);
                            return new Element<>(deserialized.getKey(), deserialized.getValue(), path, i);
                        })
                        .filter(element -> key.equals(element.key))
                        .findFirst()
        );

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

    /**
     * Removes element from disk and contents and return removed element if exists
     */
    private Optional<? extends Element<K, V>> remove(Element<K, V> element) {
        // remove from disk
        wrap(Files::deleteIfExists, StorageException.class).apply(element.path);
        // remove from contents
        Optional<List<Path>> keyPathsOpt = ofNullable(contents.get(element.key.hashCode()));
        // remove from key paths
        Optional<Element<K, V>> removedElement = keyPathsOpt
                .map(keyPaths -> {
                    keyPaths.remove(element.contentsListIndex);
                    return element;
                });
        // remove the whole key if it was the only key
        keyPathsOpt
                .filter(List::isEmpty)
                .ifPresent(keyPaths -> contents.remove(element.key.hashCode()));
        return removedElement;
    }

    @SuppressWarnings("UnusedReturnValue")
    private Element<K, V> putVal(K key, V value) {
        Path serialized = serialize(key, value);
        // update contents
        List<Path> keyPaths = ofNullable(contents.get(key.hashCode()))
                .orElseGet(() -> {
                    List<Path> newKeyPaths = new ArrayList<>();
                    contents.put(key.hashCode(), newKeyPaths);
                    return newKeyPaths;
                });
        keyPaths.add(serialized);
        return new Element<>(key, value, serialized, keyPaths.size() - 1);
    }

    private Path serialize(K key, V value) {
        Optional<List<Path>> keyPathsOpt = ofNullable(contents.get(key.hashCode()));
        String fileName = format(FILENAME_FORMAT, key.hashCode(), keyPathsOpt.map(List::size).orElse(0));
        Path serialized = folder.resolve(fileName);
        Map.Entry<K, V> entryToSerialize = new SimpleImmutableEntry<>(key, value);
        try(FileOutputStream fos = new FileOutputStream(serialized.toFile());
            ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(entryToSerialize);
            return serialized;
        } catch (Exception e) {
            throw new StorageException(e.getMessage(), e);
        }
    }

}

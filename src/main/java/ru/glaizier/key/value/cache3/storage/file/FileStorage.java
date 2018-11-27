package ru.glaizier.key.value.cache3.storage.file;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import ru.glaizier.key.value.cache3.storage.StorageException;

@NotThreadSafe
public class FileStorage<K extends Serializable, V extends Serializable> extends AbstractFileStorage<K, V> {

    public FileStorage() {
        this(TEMP_FOLDER);
    }

    public FileStorage(@Nonnull Path folder) {
        super(folder);
    }

    @Override
    protected Map<K, Path> buildContents(Path folder) throws IOException {
        return super.getFilesStream(folder)
                .collect(toMap(entry -> entry.key, entry -> entry.value, (one, another) -> one));
    }

    @Override
    public Optional<V> get(@Nonnull K key) throws StorageException {
        Objects.requireNonNull(key);

        return ofNullable(contents.get(key))
                .map(path -> deserialize(path).value);
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) throws StorageException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        // it could be written simpler but dealing with exceptions would be trickier
        Optional<Path> prevPathOpt = ofNullable(contents.get(key));
        Optional<V> prevValueOpt = prevPathOpt.map(path -> deserialize(path).value);
        Path serialized = serialize(key, value);
        contents.put(key, serialized);
        prevPathOpt.ifPresent(this::removeFile);
        return prevValueOpt;
    }

    @Override
    public Optional<V> remove(@Nonnull K key) throws StorageException {
        Objects.requireNonNull(key);

        Optional<Path> prevPathOpt = ofNullable(contents.get(key));
        Optional<V> prevValueOpt = prevPathOpt.map(path -> deserialize(path).value);
        contents.remove(key);
        prevPathOpt.ifPresent(this::removeFile);
        return prevValueOpt;
    }
}

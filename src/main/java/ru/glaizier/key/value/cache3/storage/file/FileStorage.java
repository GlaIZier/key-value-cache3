package ru.glaizier.key.value.cache3.storage.file;

import static java.util.stream.Collectors.toMap;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import ru.glaizier.key.value.cache3.storage.StorageException;

// Todo refactor to synchronized (or create synchronized wrapper)
@NotThreadSafe
public class FileStorage<K extends Serializable, V extends Serializable> extends AbstractFileStorage<K, V> {

    @Override
    protected Map<K, Path> buildContents(Path folder) throws IOException {
        return super.getFilesStream(folder)
            .collect(toMap(entry -> entry.key, entry -> entry.value, (one, another) -> one));
    }

    @Override
    public Optional<V> get(@Nonnull K key) throws StorageException {
        return null;
    }

    @Override
    public Optional<V> remove(@Nonnull K key) throws StorageException {
        return null;
    }

    @Override
    public Optional<V> put(@Nonnull K key, @Nonnull V value) throws StorageException {
        return null;
    }
}

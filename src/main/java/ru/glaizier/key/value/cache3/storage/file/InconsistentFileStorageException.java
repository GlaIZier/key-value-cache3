package ru.glaizier.key.value.cache3.storage.file;

import java.nio.file.Path;

import ru.glaizier.key.value.cache3.storage.StorageException;

/**
 * Means to take care of the inconsistent file and try to repeat the unsuccessful action again
 * @author GlaIZier
 */
public class InconsistentFileStorageException extends StorageException {

    private final Path path;

    InconsistentFileStorageException(String message, Path path) {
        super(message);
        this.path = path;
    }

    InconsistentFileStorageException(String message, Throwable cause, Path path) {
        super(message, cause);
        this.path = path;
    }

    public Path getPath() {
        return path;
    }
}

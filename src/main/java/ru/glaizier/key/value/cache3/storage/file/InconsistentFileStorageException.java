package ru.glaizier.key.value.cache3.storage.file;

import java.nio.file.Path;

/**
 * @author GlaIZier
 */
public class InconsistentFileStorageException extends RuntimeException {

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

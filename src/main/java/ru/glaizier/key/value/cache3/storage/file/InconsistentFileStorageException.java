package ru.glaizier.key.value.cache3.storage.file;

import java.nio.file.Path;
import java.util.Optional;

import ru.glaizier.key.value.cache3.storage.StorageException;

/**
 * @author GlaIZier
 */
public class InconsistentFileStorageException extends StorageException {

    private final Throwable auxiliaryException;

    private final Path path;

    InconsistentFileStorageException(String message, Throwable cause, Path path) {
        super(message, cause);
        this.path = path;
        this.auxiliaryException = null;
    }

    InconsistentFileStorageException(String message, Throwable cause, Path path, Throwable auxiliaryException) {
        super(message, cause);
        this.path = path;
        this.auxiliaryException = auxiliaryException;
    }

    public Optional<Throwable> getAuxiliaryException() {
        return Optional.ofNullable(auxiliaryException);
    }

    public Optional<Path> getPath() {
        return Optional.ofNullable(path);
    }
}

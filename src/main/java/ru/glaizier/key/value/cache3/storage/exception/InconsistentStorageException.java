package ru.glaizier.key.value.cache3.storage.exception;

import java.nio.file.Path;
import java.util.Optional;

/**
 * @author GlaIZier
 */
public class InconsistentStorageException extends StorageException {

    private final Throwable auxiliaryException;

    private final Path path;

    public InconsistentStorageException(String message) {
        super(message);
        this.auxiliaryException = null;
        this.path = null;
    }

    public InconsistentStorageException(String message, Throwable cause) {
        super(message, cause);
        this.auxiliaryException = null;
        this.path = null;
    }

    public InconsistentStorageException(String message, Throwable cause, Throwable auxiliaryException, Path path) {
        super(message, cause);
        this.auxiliaryException = auxiliaryException;
        this.path = null;
    }

    public Optional<Throwable> getAuxiliaryException() {
        return Optional.ofNullable(auxiliaryException);
    }

    public Optional<Path> getPath() {
        return Optional.ofNullable(path);
    }
}

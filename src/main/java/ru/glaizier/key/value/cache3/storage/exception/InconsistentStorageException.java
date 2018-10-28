package ru.glaizier.key.value.cache3.storage.exception;

/**
 * @author GlaIZier
 */
public class InconsistentStorageException extends StorageException {

    private final Throwable auxiliaryException;

    public InconsistentStorageException(String message) {
        super(message);
        this.auxiliaryException = null;
    }

    public InconsistentStorageException(String message, Throwable cause) {
        super(message, cause);
        this.auxiliaryException = null;
    }

    public InconsistentStorageException(String message, Throwable cause, Throwable auxiliaryException) {
        super(message, cause);
        this.auxiliaryException = auxiliaryException;
    }

    public Throwable getAuxiliaryException() {
        return auxiliaryException;
    }
}

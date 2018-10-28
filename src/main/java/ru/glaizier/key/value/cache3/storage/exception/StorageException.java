package ru.glaizier.key.value.cache3.storage.exception;

/**
 * @author GlaIZier
 */
public class StorageException extends RuntimeException {
    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}

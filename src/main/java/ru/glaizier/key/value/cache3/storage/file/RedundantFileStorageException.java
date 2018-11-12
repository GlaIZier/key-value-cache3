package ru.glaizier.key.value.cache3.storage.file;

import java.nio.file.Path;

/**
 * Means that the main operation was successful, but exception occurred during clean-up process (deleting file) to show
 * that you need to take care of the file manually
 */
public class RedundantFileStorageException extends InconsistentFileStorageException {

    RedundantFileStorageException(String message, Path path) {
        super(message, path);
    }

    RedundantFileStorageException(String message, Throwable cause, Path path) {
        super(message, cause, path);
    }
}

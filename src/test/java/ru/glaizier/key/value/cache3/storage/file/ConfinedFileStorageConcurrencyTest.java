package ru.glaizier.key.value.cache3.storage.file;

import java.io.Serializable;
import java.nio.file.Path;

import ru.glaizier.key.value.cache3.storage.Storage;

/**
 * @author GlaIZier
 */
public class ConfinedFileStorageConcurrencyTest extends AbstractFileStorageConcurrencyTest {

    protected <K extends Serializable, V extends Serializable> Storage<K, V> getStorage(Path folder) {
        return new ConfinedFileStorage<>(folder);
    }

    @Override
    protected <K extends Serializable, V extends Serializable> boolean cleanUpStorage(Storage<K, V> storage) {
        try {
            return ((ConfinedFileStorage) storage).stopDiskWorker();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }
}

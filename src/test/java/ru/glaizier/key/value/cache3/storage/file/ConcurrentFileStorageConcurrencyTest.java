package ru.glaizier.key.value.cache3.storage.file;

import java.io.Serializable;
import java.nio.file.Path;

import ru.glaizier.key.value.cache3.storage.Storage;

/**
 * @author GlaIZier
 */
public class ConcurrentFileStorageConcurrencyTest extends FileStorageConcurrencyTest {

    protected <K extends Serializable, V extends Serializable> Storage<K, V> getStorage(Path folder) {
        return new ConcurrentFileStorage<>(folder);
    }

    @Override
    protected <K extends Serializable, V extends Serializable> boolean cleanUpStorage(Storage<K, V> storage) {
        return true;
    }
}

package ru.glaizier.key.value.cache3.storage.file;

import java.io.Serializable;
import java.nio.file.Path;

import ru.glaizier.key.value.cache3.storage.Storage;
import ru.glaizier.key.value.cache3.storage.SynchronousStorage;

/**
 * @author GlaIZier
 */
public class SynchronousFileStorageConcurrencyTest extends FileStorageConcurrencyTest {

    protected <K extends Serializable, V extends Serializable> Storage<K, V> getStorage(Path folder) {
        return new SynchronousStorage<>(new FileStorage<>(folder));
    }

    @Override
    protected <K extends Serializable, V extends Serializable> boolean cleanUpStorage(Storage<K, V> storage) {
        return true;
    }
}

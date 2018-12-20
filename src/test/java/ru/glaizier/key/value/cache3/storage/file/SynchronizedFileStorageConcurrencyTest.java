package ru.glaizier.key.value.cache3.storage.file;

import java.io.Serializable;
import java.nio.file.Path;

import ru.glaizier.key.value.cache3.storage.Storage;
import ru.glaizier.key.value.cache3.storage.SynchronizedStorage;

/**
 * @author GlaIZier
 */
public class SynchronizedFileStorageConcurrencyTest extends AbstractFileStorageConcurrencyTest {

    protected <K extends Serializable, V extends Serializable> Storage<K, V> getStorage(Path folder) {
        return new SynchronizedStorage<>(new FileStorage<>(folder));
    }

    @Override
    protected <K extends Serializable, V extends Serializable> boolean cleanUpStorage(Storage<K, V> storage) {
        return true;
    }
}

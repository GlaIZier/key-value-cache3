package ru.glaizier.key.value.cache3.storage.file;

import org.junit.Before;

/**
 * @author GlaIZier
 */
public class ConcurrentFileStorageTest extends AbstractFileStorageTest {

    @Before
    public void init() {
        storage = new ConcurrentFileStorage<>(temporaryFolder.getRoot().toPath());
        collisionsStorage = new ConcurrentFileStorage<>(temporaryFolder.getRoot().toPath());
    }

}

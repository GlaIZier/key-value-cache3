package ru.glaizier.key.value.cache3.storage.file;

import org.junit.Before;

/**
 * @author GlaIZier
 */
public class FileStorageTest extends AbstractFileStorageTest {

    @Before
    public void init() {
        storage = new FileStorage<>(temporaryFolder.getRoot().toPath());
        collisionsStorage = new FileStorage<>(temporaryFolder.getRoot().toPath());
    }

}

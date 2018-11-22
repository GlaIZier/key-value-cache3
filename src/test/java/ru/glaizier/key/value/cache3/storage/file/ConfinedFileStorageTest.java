package ru.glaizier.key.value.cache3.storage.file;

import org.junit.After;
import org.junit.Before;

/**
 * @author GlaIZier
 */
public class ConfinedFileStorageTest extends FileStorageTest {

    @Before
    public void init() {
        storage = new ConfinedFileStorage<>(temporaryFolder.getRoot().toPath());
        collisionsStorage = new ConfinedFileStorage<>(temporaryFolder.getRoot().toPath());
    }

    @After
    public void shutdown() throws InterruptedException {
        ((ConfinedFileStorage) storage).stopDiskWorker();
        ((ConfinedFileStorage) collisionsStorage).stopDiskWorker();
    }

}

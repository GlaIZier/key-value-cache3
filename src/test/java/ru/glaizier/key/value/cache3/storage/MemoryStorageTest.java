package ru.glaizier.key.value.cache3.storage;

import org.junit.Test;

/**
 * @author GlaIZier
 */
public class MemoryStorageTest {

    @Test
    public void t() {
        MemoryStorage<Integer, String> map = new MemoryStorage<>();
        map.get(null);
    }

}

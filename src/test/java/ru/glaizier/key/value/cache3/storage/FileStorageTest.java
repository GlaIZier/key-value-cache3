package ru.glaizier.key.value.cache3.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author GlaIZier
 */
public class FileStorageTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Storage<Integer, String> storage;

    private Storage<HashCodeEqualsPojo, String> collisionsStorage;

    private static class HashCodeEqualsPojo implements Serializable {
        private final int i;
        private final String s;

        private HashCodeEqualsPojo(int i, String s) {
            Objects.requireNonNull(s, "s");
            this.i = i;
            this.s = s;
        }

        int getI() {
            return i;
        }

        String getS() {
            return s;
        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public boolean equals(Object that) {
            if (super.equals(that))
                return true;
            if (!(that instanceof HashCodeEqualsPojo))
                return false;
            HashCodeEqualsPojo castedThat = (HashCodeEqualsPojo) that;
            return (castedThat.getI() == this.getI()) && castedThat.getS().equals(this.getS());
        }
    }

    @Before
    public void init() {
        storage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
        collisionsStorage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());
    }

    /*
    @Test
    public void createContents() throws IOException {
        IntStream.rangeClosed(1, 2).forEach(i -> {
            try {
                temporaryFolder.newFile(format(FileStorageConcurrent.FILENAME_FORMAT, i, 0));
                temporaryFolder.newFile(format(FileStorageConcurrent.FILENAME_FORMAT, i, 1));
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });
        temporaryFolder.newFile("somefile.ser");
        Map<Integer, List<Path>> contents = FileStorageConcurrent(temporaryFolder.getRoot().toPath());

        assertThat(contents.size(), is(2));
        assertThat(contents.get(1).size(), is(2));
        assertThat(contents.get(1).get(0).toString(), not(isEmptyOrNullString()));
        assertThat(contents.get(1).get(1).toString(), not(isEmptyOrNullString()));
        assertThat(contents.get(2).size(), is(2));
        assertThat(contents.get(2).get(0).toString(), not(isEmptyOrNullString()));
        assertThat(contents.get(2).get(1).toString(), not(isEmptyOrNullString()));
    }

    @Test
    public void createContentsWhenFolderEmpty() throws IOException {
        Map<Integer, List<Path>> contents = FileStorage.createContents(temporaryFolder.getRoot().toPath());
        assertTrue(contents.isEmpty());
    }
    */


    @Test
    public void put() {
        storage.put(1, "1");
        storage.put(2, "2");

        assertThat(storage.getSize(), is(2));
        assertTrue(storage.contains(1));
        assertTrue(storage.contains(2));
        assertFalse(storage.contains(3));

        // put with the same key
        storage.put(1, "3");
        assertThat(storage.getSize(), is(2));
        assertTrue(storage.contains(1));
        assertTrue(storage.contains(2));
        assertFalse(storage.contains(3));
    }

    @Test
    public void get() {
        storage.put(1, "1");
        storage.put(2, "2");

        assertThat(storage.get(1), is(Optional.of("1")));
        assertThat(storage.get(2), is(Optional.of("2")));
        assertThat(storage.get(3), is(Optional.empty()));

        // put with the same key
        storage.put(1, "3");
        assertThat(storage.get(1), is(Optional.of("3")));
    }

    @Test
    public void remove() {
        storage.put(1, "1");
        storage.put(2, "2");

        assertThat(storage.remove(1), is(Optional.of("1")));
        assertThat(storage.get(1), is(Optional.empty()));
        assertThat(storage.getSize(), is(1));
        assertFalse(storage.contains(1));

        assertThat(storage.remove(2), is(Optional.of("2")));
        assertThat(storage.get(2), is(Optional.empty()));
        assertThat(storage.getSize(), is(0));
        assertFalse(storage.contains(2));

        assertThat(storage.remove(2), is(Optional.empty()));
        assertThat(storage.get(2), is(Optional.empty()));
        assertThat(storage.getSize(), is(0));
        assertFalse(storage.contains(2));
    }

    @Test
    public void putWithCollisions() {
        HashCodeEqualsPojo key10 = new HashCodeEqualsPojo(1, "0");
        HashCodeEqualsPojo key11 = new HashCodeEqualsPojo(1, "1");
        HashCodeEqualsPojo key20 = new HashCodeEqualsPojo(2, "0");
        collisionsStorage.put(key10, "10");
        collisionsStorage.put(key11, "11");
        collisionsStorage.put(key20, "20");

        assertThat(collisionsStorage.getSize(), is(3));
        assertTrue(collisionsStorage.contains(key10));
        assertTrue(collisionsStorage.contains(key11));
        assertTrue(collisionsStorage.contains(key20));

        // put with the same key
        collisionsStorage.put(key20, "21");
        assertThat(collisionsStorage.getSize(), is(3));
        assertTrue(collisionsStorage.contains(key10));
        assertTrue(collisionsStorage.contains(key11));
        assertTrue(collisionsStorage.contains(key20));
    }

    @Test
    public void getWithCollisions() {
        HashCodeEqualsPojo key10 = new HashCodeEqualsPojo(1, "0");
        HashCodeEqualsPojo key11 = new HashCodeEqualsPojo(1, "1");
        HashCodeEqualsPojo key12 = new HashCodeEqualsPojo(1, "2");
        HashCodeEqualsPojo key20 = new HashCodeEqualsPojo(2, "0");
        collisionsStorage.put(key10, "10");
        collisionsStorage.put(key11, "11");
        collisionsStorage.put(key12, "11");
        collisionsStorage.put(key20, "20");

        assertThat(collisionsStorage.get(key10), is(Optional.of("10")));
        assertThat(collisionsStorage.get(key11), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key12), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key20), is(Optional.of("20")));

        // put with the same key
        collisionsStorage.put(key20, "21");
        collisionsStorage.put(key11, "13");
        assertThat(collisionsStorage.get(key20), is(Optional.of("21")));
        assertThat(collisionsStorage.get(key11), is(Optional.of("13")));
    }

    @Test
    public void removeWithCollisions() {
        HashCodeEqualsPojo key10 = new HashCodeEqualsPojo(1, "0");
        HashCodeEqualsPojo key11 = new HashCodeEqualsPojo(1, "1");
        HashCodeEqualsPojo key12 = new HashCodeEqualsPojo(1, "2");
        HashCodeEqualsPojo key20 = new HashCodeEqualsPojo(2, "0");
        collisionsStorage.put(key10, "10");
        collisionsStorage.put(key11, "11");
        collisionsStorage.put(key12, "11");
        collisionsStorage.put(key20, "20");

        assertThat(collisionsStorage.remove(key10), is(Optional.of("10")));
        assertThat(collisionsStorage.get(key10), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(3));
        assertFalse(collisionsStorage.contains(key10));

        assertThat(collisionsStorage.remove(key11), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key11), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(2));
        assertFalse(collisionsStorage.contains(key11));

        assertThat(collisionsStorage.remove(key12), is(Optional.of("11")));
        assertThat(collisionsStorage.get(key12), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(1));
        assertFalse(collisionsStorage.contains(key12));

        assertThat(collisionsStorage.remove(key20), is(Optional.of("20")));
        assertThat(collisionsStorage.get(key20), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(0));
        assertFalse(collisionsStorage.contains(key20));

        assertThat(collisionsStorage.remove(key20), is(Optional.empty()));
        assertThat(collisionsStorage.get(key20), is(Optional.empty()));
        assertThat(collisionsStorage.getSize(), is(0));
        assertFalse(collisionsStorage.contains(key20));
    }

    @Test
    public void buildContents() {
        // Files that don't stick to FileName pattern or can't be deserialized won't appear in contents
        IntStream.rangeClosed(1, 2).forEach(i -> {
            try {
                temporaryFolder.newFile(UUID.randomUUID().toString() + ".ser");
                temporaryFolder.newFile(i + "#" + UUID.randomUUID().toString() + ".ser");
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });

        Storage<Integer, String> localStorage = new FileStorageConcurrent<>(temporaryFolder.getRoot().toPath());

        assertTrue(localStorage.isEmpty());
    }

}

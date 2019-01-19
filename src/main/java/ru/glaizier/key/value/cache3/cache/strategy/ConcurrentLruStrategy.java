package ru.glaizier.key.value.cache3.cache.strategy;

import ru.glaizier.key.value.cache3.util.Entry;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.*;

/**
 * @author GlaIZier
 */
@ThreadSafe
public class ConcurrentLruStrategy<K> implements Strategy<K> {

    private static final int FIRST_VERSION = 1;

    @GuardedBy("running")
    // There can be duplicates in q. This is done to speed up use operation
    private final Queue<Entry<K, Integer>> q = new ConcurrentLinkedQueue<>();

    // Since there can be duplicates in the q, this map is used to show if a key is present in the queue.
    // null - not present (q contains duplicate, but this value has already been evicted). Integer - the last index of this key.
    // when we peek the element from the queue, we compare its integer and an integer here to decide whether we peeked the last element.
    // <1-3> -> <2-2> -> <1-1>. The first candidate is <1-1>, but we have it later (<1-3>: keysToVersion contains 1:3), so it's not the right candidate
    private final ConcurrentMap<K, Integer> keysToVersion = new ConcurrentHashMap<>();

    // Crucial map to guard simultaneous update of q and keysToVersion
    private final ConcurrentMap<K, Operation> running = new ConcurrentHashMap<>();

    @Override
    public Optional<K> evict() {
        UUID peekHash = UUID.randomUUID();
        Entry<K, Integer> peeked;
        // get not duplicated key
        while (true) {
            Entry<K, Integer> currentPeeked = q.peek();
            if (currentPeeked == null)
                return empty();

            Operation operation = running
                .computeIfAbsent(currentPeeked.key, (peekedLocal) -> new PeekOperation(currentPeeked, peekHash));
            if (operation.hash.equals(peekHash)) {
                // if true then we found not duplicated key (key to peek)
                if (operation.execute()) {
                    peeked = currentPeeked;
                    break;
                }
            } else {
                // help to execute another task
                operation.execute();
            }
        }

        UUID evictHash = UUID.randomUUID();
        while (true) {
            Operation operation = running
                .computeIfAbsent(peeked.key, (peekedLocal) -> new EvictOperation(peeked, evictHash));
            if (operation.hash.equals(evictHash)) {
                // execute the target task
                while (true) {
                    if (operation.execute()) {
                        return of(peeked.key);
                    }
                }
            } else {
                // help to execute another task
                operation.execute();
            }
        }
    }


    /**
     * O(1)
     */
    @Override
    public boolean use(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        UUID evictHash = UUID.randomUUID();
        while (true) {
            Operation operation = running.computeIfAbsent(key, (k) -> new UseOperation(
                new Entry<>(key, ofNullable(keysToVersion.get(key)).map(version -> version + 1).orElse(FIRST_VERSION)),
                evictHash));
            if (operation.hash.equals(evictHash)) {
                // execute the target task
                while (true) {
                    if (operation.execute())
                        return ((ResultOperation)operation).getResult();
                }
            } else {
                // help to execute another task
                operation.execute();
            }
        }
    }

    /**
     * O(n) * number of keys in the queue
     */
    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        UUID removeHash = UUID.randomUUID();
        while (true) {
            Operation operation = running.computeIfAbsent(key, (k) -> new RemoveOperation(
                new Entry<>(key, ofNullable(keysToVersion.get(key)).orElse(FIRST_VERSION)), removeHash));
            if (operation.hash.equals(removeHash)) {
                // execute the target task
                while (true) {
                    if (operation.execute()) {
                        return ((ResultOperation) operation).getResult();
                    }
                }
            } else {
                // help to execute another task
                operation.execute();
            }
        }
    }

    private abstract class Operation {
        protected final Entry<K, Integer> keyToVersion;
        private final UUID hash;
        // this can be replaced with a list of AtomicBoolean
        protected final AtomicBoolean firstStarted = new AtomicBoolean(false);
        protected final AtomicBoolean firstDone = new AtomicBoolean(false);
        protected final AtomicBoolean secondStarted = new AtomicBoolean(false);
        protected final AtomicBoolean secondDone = new AtomicBoolean(false);
        protected final AtomicBoolean done = new AtomicBoolean(false);

        private Operation(Entry<K, Integer> keyToVersion, UUID hash) {
            this.keyToVersion = keyToVersion;
            this.hash = hash;
        }

        public abstract boolean execute();
    }

    private abstract class ResultOperation extends Operation {
        protected final AtomicBoolean result = new AtomicBoolean(false);

        private ResultOperation(Entry<K, Integer> keyToVersion, UUID hash) {
            super(keyToVersion, hash);
        }

        public abstract boolean execute();

        private boolean getResult() {
            return result.get();
        }
    }

    /**
     * Operation to peek not duplicated key
     */
    private class PeekOperation extends Operation {
        private final AtomicBoolean isSuccessful = new AtomicBoolean(false);

        private PeekOperation(Entry<K, Integer> keyToVersion, UUID hash) {
            super(keyToVersion, hash);
        }

        @Override
        public boolean execute() {
            if (firstStarted.compareAndSet(false, true)) {
                if (keyToVersion.value.equals(keysToVersion.get(keyToVersion.key))) {
                    // this peeked key is what we need
                    isSuccessful.set(true);
                } else {
                    // this key in q is a duplicate and we need to remove it
                    q.remove(keyToVersion);
                    isSuccessful.set(false);
                }
                done.set(true);
                running.remove(keyToVersion.key);
            }
            return isSuccessful.get();
        }
    }

    private class EvictOperation extends Operation {
        private EvictOperation(Entry<K, Integer> keyToVersion, UUID hash) {
            super(keyToVersion, hash);
        }

        @Override
        public boolean execute() {
            if (done.get())
                return true;
            if (firstStarted.compareAndSet(false, true)) {
                q.remove(keyToVersion);
                firstDone.set(true);
            }
            if (secondStarted.compareAndSet(false, true)) {
                keysToVersion.remove(keyToVersion.key);
                secondDone.set(true);
            }
            if (firstDone.get() && secondDone.get()) {
                if (done.compareAndSet(false, true)) {
                    running.remove(keyToVersion.key);
                    return true;
                }
            }
            return false;
        }
    }

    private class UseOperation extends ResultOperation {

        private UseOperation(Entry<K, Integer> keyToVersion, UUID hash) {
            super(keyToVersion, hash);
        }

        @Override
        public boolean execute() {
            if (done.get())
                return true;
            if (firstStarted.compareAndSet(false, true)) {
                q.add(keyToVersion);
                firstDone.set(true);
            }
            if (secondStarted.compareAndSet(false, true)) {
                Integer prevVersion = keysToVersion.put(keyToVersion.key, keyToVersion.value);
                if (prevVersion != null)
                    result.set(true);
                secondDone.set(true);
            }
            if (firstDone.get() && secondDone.get()) {
                if (done.compareAndSet(false, true)) {
                    running.remove(keyToVersion.key);
                    return true;
                }
            }
            return false;
        }
    }

    private class RemoveOperation extends ResultOperation {
        private RemoveOperation(Entry<K, Integer> keyToVersion, UUID hash) {
            super(keyToVersion, hash);
        }

        @Override
        public boolean execute() {
            if (done.get()) {
                return true;
            }
            if (firstStarted.compareAndSet(false, true)) {
                Integer version = keysToVersion.remove(keyToVersion.key);
                result.set(version != null);
                firstDone.set(true);
            }
            if (secondStarted.compareAndSet(false, true)) {
                for (Integer currentVersion = keyToVersion.value; currentVersion > 0; currentVersion--) {
                    if (!q.remove(new Entry<>(keyToVersion.key, currentVersion)))
                        break;
                }
                secondDone.set(true);
            }
            if (firstDone.get() && secondDone.get()) {
                if (done.compareAndSet(false, true)) {
                    running.remove(keyToVersion.key);
                    return true;
                }
            }
            return false;
        }
    }
}

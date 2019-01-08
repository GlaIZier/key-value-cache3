package ru.glaizier.key.value.cache3.cache.strategy;

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

import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * @author GlaIZier
 */
@ThreadSafe
public class ConcurrentLruStrategy1<K> implements Strategy<K> {

    @GuardedBy("running")
    // There can be duplicates in q. This is done to speed up use operation
    private final Queue<K> q = new ConcurrentLinkedQueue<>();

    // Since there can be duplicates in the q, this map is used to show if a key is present in q.
    // null - not present (q contains duplicate, but this value has already been evicted). True - present
    private final ConcurrentMap<K, Boolean> keys = new ConcurrentHashMap<>();

    // Crucial map to guard simultaneous update of q and keys
    private final ConcurrentMap<K, Operation> running = new ConcurrentHashMap<>();

    @Override
    public Optional<K> evict() {
        K toEvict;
        UUID peekHash = UUID.randomUUID();
        // get not duplicated key
        while (true) {
            toEvict = q.peek();
            if (toEvict == null)
                return empty();

            Operation operation = running.computeIfAbsent(toEvict, (k) -> new PeekOperation(toEvict, peekHash));
            if (operation.hash.equals(peekHash)) {
                // if true then we found not duplicated key (key to peek)
                if (operation.execute())
                    break;
            } else {
                // help to execute another task
                operation.execute();
            }
        }

        UUID evictHash = UUID.randomUUID();
        while (true) {
            Operation operation = running.computeIfAbsent(toEvict, (k) -> new EvictOperation(toEvict, evictHash));
            if (operation.hash.equals(evictHash)) {
                // execute the target task
                while (true) {
                    if (operation.execute())
                        return of(toEvict);
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
        q.add(key);
        Boolean prev = keys.put(key, true);

        return prev != null;
    }

    /**
     * O(n) * number of keys in the queue
     */
    @Override
    public boolean remove(@Nonnull K key) {
        Objects.requireNonNull(key, "key");

        Boolean exists = keys.remove(key);
        if (exists != null)
            while (true)
                if (!q.remove(key))
                    break;
        return exists != null;
    }

    private abstract class Operation {
        protected final K key;
        private final UUID hash;
        // Todo refactor to the list of operations?
        protected final AtomicBoolean firstStarted = new AtomicBoolean(false);
        protected final AtomicBoolean firstDone = new AtomicBoolean(false);
        protected final AtomicBoolean secondStarted = new AtomicBoolean(false);
        protected final AtomicBoolean secondDone = new AtomicBoolean(false);
        protected final AtomicBoolean done = new AtomicBoolean(false);

        private Operation(K key, UUID hash) {
            this.key = key;
            this.hash = hash;
        }

        public abstract boolean execute();
    }

    /**
     * Operation to peek not duplicated key
     */
    private class PeekOperation extends Operation {
        private final AtomicBoolean isSuccessful = new AtomicBoolean(false);

        private PeekOperation(K key, UUID hash) {
            super(key, hash);
        }

        @Override
        public boolean execute() {
            if (firstStarted.compareAndSet(false, true)) {
                if (keys.get(key) != null) {
                    // this peeked key is what we need
                    isSuccessful.set(true);
                } else {
                    // this key in q is a duplicate and we need to remove it
                    q.remove(key);
                    isSuccessful.set(false);
                }
                done.set(true);
                running.remove(key);
            }
            return isSuccessful.get();
        }
    }

    private class EvictOperation extends Operation {
        private EvictOperation(K key, UUID hash) {
            super(key, hash);
        }

        @Override
        public boolean execute() {
            if (done.get())
                return true;
            if (firstStarted.compareAndSet(false, true)) {
                q.remove(key);
                firstDone.set(true);
            }
            if (secondStarted.compareAndSet(false, true)) {
                keys.remove(key);
                secondDone.set(true);
            }
            if (firstDone.get() && secondDone.get()) {
                if (done.compareAndSet(false, true)) {
                    running.remove(key);
                    return true;
                }
            }
            return false;
        }
    }

    private class UseOperation extends Operation {
        private UseOperation(K key, UUID hash) {
            super(key, hash);
        }

        @Override
        public boolean execute() {
            if (done.get())
                return true;
            if (firstStarted.compareAndSet(false, true)) {
                q.remove(key);
                firstDone.set(true);
            }
            if (secondStarted.compareAndSet(false, true)) {
                keys.put(key, false);
                secondDone.set(true);
            }
            if (firstDone.get() && secondDone.get()) {
                if (done.compareAndSet(false, true)) {
                    running.remove(key);
                    return true;
                }
            }
            return false;
        }
    }
}

package io.basestar.storage;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public interface Coordinator {

    default CloseableLock lock(final String ... names) {

        return lock(ImmutableSet.copyOf(names));
    }

    CloseableLock lock(final Set<String> names);

    class NoOp implements Coordinator {

        @Override
        public CloseableLock lock(final Set<String> name) {

            return () -> {};
        }
    }

    class Local implements Coordinator {

        private final ConcurrentMap<String, Lock> locks = new ConcurrentHashMap<>();

        @Override
        public CloseableLock lock(final Set<String> names) {

            final List<Lock> locked = names.stream().sorted().map(name -> locks.computeIfAbsent(name, ignored -> new ReentrantLock()))
                    .collect(Collectors.toList());
            locked.forEach(Lock::lock);
            return () -> locked.forEach(Lock::unlock);
        }
    }
}

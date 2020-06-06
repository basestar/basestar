package io.basestar.storage.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import io.basestar.storage.CloseableLock;
import io.basestar.storage.Coordinator;
import io.basestar.util.Nullsafe;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

public class HazelcastCoordinator implements Coordinator {

    private final HazelcastInstance instance;

    @lombok.Builder(builderClassName = "Builder")
    public HazelcastCoordinator(@Nonnull final HazelcastInstance instance) {

        this.instance = Nullsafe.require(instance);
    }

    @Override
    public CloseableLock lock(final Set<String> names) {

        final CPSubsystem subsystem = instance.getCPSubsystem();

        final List<Lock> locked = names.stream().sorted().map(subsystem::getLock)
                .collect(Collectors.toList());
        locked.forEach(Lock::lock);
        return () -> locked.forEach(Lock::unlock);
    }
}

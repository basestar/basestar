package io.basestar.storage.hazelcast;

/*-
 * #%L
 * basestar-storage-hazelcast
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
    HazelcastCoordinator(@Nonnull final HazelcastInstance instance) {

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

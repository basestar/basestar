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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.storage.hazelcast.serde.PortableSchemaFactory;
import io.basestar.util.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


class TestHazelcastStorage extends TestStorage {

    private static HazelcastInstance INSTANCE;

    private static HazelcastInstance instance(final Config config) {

        if(INSTANCE == null) {

            INSTANCE = Hazelcast.newHazelcastInstance(config);
        }
        return INSTANCE;
    }

    @Override
    protected Storage storage(final Namespace namespace) {

        final String suffix = "-" + UUID.randomUUID().toString();

        final HazelcastStrategy.Simple strategy = HazelcastStrategy.Simple.builder()
                .objectPrefix("object-")
                .objectSuffix(suffix)
                .historyPrefix("history-")
                .historySuffix(suffix)
                .build();


        final PortableSchemaFactory recordFactory = new PortableSchemaFactory(1, namespace);

        final Config config = new Config()
                .setSerializationConfig(recordFactory.serializationConfig())
                .setMapConfigs(strategy.mapConfigs(namespace));

        final HazelcastInstance instance = instance(config);

        final Storage storage = HazelcastStorage.builder()
                .setInstance(instance)
                .setStrategy(strategy)
                .setSchemaFactory(recordFactory)
                .build();

        return storage;
    }

    @Override
    protected void testMultiValueIndex() {

    }

    @Override
    protected void testLarge() {

    }

    // FIXME
    @Override
    protected void testSortAndPaging() {

    }

    // FIXME
    @Override
    protected void testSortAndPagingNulls() {

    }
}

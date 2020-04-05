package io.basestar.storage.hazelcast;

import com.google.common.collect.Multimap;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.basestar.schema.Namespace;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.storage.hazelcast.serde.PortableSchemaFactory;

import java.util.Map;
import java.util.UUID;


public class TestHazelcastStorage extends TestStorage {

    private static HazelcastInstance INSTANCE;

    private static HazelcastInstance instance(final Config config) {

        if(INSTANCE == null) {

            INSTANCE = Hazelcast.newHazelcastInstance(config);



//                    .setSerializationConfig(new SerializationConfig()
//                            .setPortableFactories(ImmutableMap.of(
//                                    1, recordFactory
//                            ))));
        }
        return INSTANCE;
    }

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final String suffix = "-" + UUID.randomUUID().toString();

        final HazelcastRouting.Simple routing = HazelcastRouting.Simple.builder()
                .objectPrefix("object-")
                .objectSuffix(suffix)
                .historyPrefix("history-")
                .historySuffix(suffix)
                .build();


        final PortableSchemaFactory recordFactory = new PortableSchemaFactory(1, namespace);

        final Config config = new Config()
                .setSerializationConfig(recordFactory.serializationConfig())
                .setMapConfigs(routing.mapConfigs(namespace));
//                .addMapConfig(new MapConfig()
//                        .setName("*")
//                        .addEntryListenerConfig(new EntryListenerConfig()
//                        .setImplementation(new EntryListener<Void, Void>() {
//                            @Override
//                            public void mapEvicted(final MapEvent mapEvent) {
//
//                            }
//
//                            @Override
//                            public void mapCleared(final MapEvent mapEvent) {
//
//                            }
//
//                            @Override
//                            public void entryUpdated(final EntryEvent entryEvent) {
//
//                            }
//
//                            @Override
//                            public void entryRemoved(final EntryEvent entryEvent) {
//
//                            }
//
//                            @Override
//                            public void entryExpired(final EntryEvent entryEvent) {
//
//                            }
//
//                            @Override
//                            public void entryEvicted(final EntryEvent entryEvent) {
//
//                            }
//
//                            @Override
//                            public void entryAdded(final EntryEvent entryEvent) {
//
//                            }
//                        })));

        final HazelcastInstance instance = instance(config);

//        for(final Schema<?> schema : namespace.getSchemas().values()) {
//            if(schema instanceof ObjectSchema) {
//                final ObjectSchema objectSchema = (ObjectSchema)schema;
//                final String name = routing.objectMapName(objectSchema);
//                final IMap<BatchResponse.Key, Map<String, Object>> map = instance.getMap(name);
//                final List<IndexConfig> indexes = PortableSchemaFactory.indexes(objectSchema);
//                indexes.forEach(map::addIndex);
//            }
//        }

        final Storage storage = HazelcastStorage.builder()
                .setInstance(instance)
                .setRouting(routing)
                .setSchemaFactory(recordFactory)
                .build();

        writeAll(storage, namespace, data);

        return storage;
    }

    @Override
    public void testMultiValueIndex() {

    }

    @Override
    public void testLarge() {

    }
}

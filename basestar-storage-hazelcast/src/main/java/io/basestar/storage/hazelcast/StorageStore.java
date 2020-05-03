//package io.basestar.storage.hazelcast;

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
//
//import com.hazelcast.map.MapStore;
//import io.basestar.schema.Consistency;
//import io.basestar.schema.Instance;
//import io.basestar.schema.Namespace;
//import io.basestar.schema.ObjectSchema;
//import io.basestar.storage.BatchResponse;
//import io.basestar.storage.Storage;
//import io.basestar.storage.exception.ObjectExistsException;
//import io.basestar.storage.hazelcast.serde.CustomPortable;
//import io.basestar.storage.hazelcast.serde.PortableSchemaFactory;
//
//import java.util.Collection;
//import java.util.Map;
//
//public class StorageStore implements MapStore<BatchResponse.Key, CustomPortable> {
//
//    private final PortableSchemaFactory factory;
//
//    private final Namespace namespace;
//
//    private final Storage storage;
//
//    public StorageStore(final PortableSchemaFactory factory, final Namespace namespace, final Storage storage) {
//
//        this.factory = factory;
//        this.namespace = namespace;
//        this.storage = storage;
//    }
//
//    @Override
//    public void store(final BatchResponse.Key key, final CustomPortable value) {
//
//        final Storage.WriteTransaction write = storage.write(Consistency.NONE);
//        final ObjectSchema schema = namespace.requireObjectSchema(key.getSchema());
//        final Map<String, Object> after = value.getData();
//        if(key.getVersion() == null) {
//            final Long version = Instance.getVersion(after);
//            assert version != null;
//            if(version == 1L) {
//                try {
//                    write.createObject(schema, key.getId(), after);
//                } catch (final ObjectExistsException e) {
//                    write.updateObject(schema, key.getId(), null, after);
//                }
//            } else {
//                write.updateObject(schema, key.getId(), null, after);
//            }
//        } else {
//            final Long version = Instance.getVersion(after);
//            assert version != null;
//            write.createHistory(schema, key.getId(), version, after);
//        }
//        write.commit().join();
//    }
//
//    @Override
//    public void storeAll(final Map<BatchResponse.Key, CustomPortable> entries) {
//
//        entries.forEach(this::store);
//    }
//
//    @Override
//    public void delete(final BatchResponse.Key key) {
//
//        final Storage.WriteTransaction write = storage.write(Consistency.NONE);
//        final ObjectSchema schema = namespace.requireObjectSchema(key.getSchema());
//        write.deleteObject(schema, key.getId(), null);
//        write.commit().join();
//    }
//
//    @Override
//    public void deleteAll(final Collection<BatchResponse.Key> keys) {
//
//        keys.forEach(this::delete);
//    }
//
//    @Override
//    public CustomPortable load(final BatchResponse.Key key) {
//
//        final ObjectSchema schema = namespace.requireObjectSchema(key.getSchema());
//        if(key.getVersion() == null) {
//            final CustomPortable portable = factory.create(schema);
//            portable.setData(storage.readObject(schema, key.getId()).join());
//            return portable;
//        } else {
//            final CustomPortable portable = factory.create(schema);
//            portable.setData(storage.readObjectVersion(schema, key.getId(), key.getVersion()).join());
//            return portable;
//        }
//    }
//
//    @Override
//    public Map<BatchResponse.Key, CustomPortable> loadAll(final Collection<BatchResponse.Key> keys) {
//
//        return null;
//    }
//
//    @Override
//    public Iterable<BatchResponse.Key> loadAllKeys() {
//
//        return null;
//    }
//}

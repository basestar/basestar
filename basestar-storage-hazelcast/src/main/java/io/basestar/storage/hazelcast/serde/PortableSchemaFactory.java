package io.basestar.storage.hazelcast.serde;

/*-
 * #%L
 * basestar-storage-hazelcast
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.PortableFactory;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Schema;

import java.util.*;

public class PortableSchemaFactory implements PortableFactory {

    public static final int REF_SLOT = 1;

    public static final int SLOT_OFFSET = 2;

    private final int factoryId;

    private final Map<Integer, SortedMap<String, AttributeType<?>>> slots = new HashMap<>();

    public PortableSchemaFactory(final int factoryId, final Namespace namespace) {

        this.factoryId = factoryId;

        slots.put(REF_SLOT, refAttributes());
        for(final Schema schema : namespace.getSchemas().values()) {
            if(schema instanceof InstanceSchema) {
                final int slot = schema.getSlot() + SLOT_OFFSET;
                final SortedMap<String, AttributeType<?>> attributes = attributes((InstanceSchema)schema);
                slots.put(slot, attributes);
            }
        }
    }

    public Set<ClassDefinition> defs() {

        final Set<ClassDefinition> defs = new HashSet<>();
        for(final Map.Entry<Integer, SortedMap<String, AttributeType<?>>> entry : slots.entrySet()) {
            defs.add(def(entry.getKey(), entry.getValue()));
        }
        return defs;
    }

    private ClassDefinition def(final int classId, final Map<String, AttributeType<?>> attributes) {

        final ClassDefinitionBuilder builder = new ClassDefinitionBuilder(factoryId, classId);
        attributes.forEach((name, attr) -> attr.def(this, builder, name));
        return builder.build();
    }

    private static SortedMap<String, AttributeType<?>> refAttributes() {

        final SortedMap<String, AttributeType<?>> attributes = new TreeMap<>();
        ObjectSchema.REF_SCHEMA.forEach((k, v) -> {
            final AttributeType<?> type = v.visit(AttributeTypeVisitor.INSTANCE);
            attributes.put(k, type);
        });
        return attributes;
    }

    private static SortedMap<String, AttributeType<?>> attributes(final InstanceSchema schema) {

        final SortedMap<String, AttributeType<?>> attributes = new TreeMap<>();
        schema.metadataSchema().forEach((k, v) -> {
            final AttributeType<?> type = v.visit(AttributeTypeVisitor.INSTANCE);
            attributes.put(k, type);
        });
        schema.getAllProperties().forEach((k, v) -> {
            final AttributeType<?> type = v.getType().visit(AttributeTypeVisitor.INSTANCE);
            attributes.put(k, type);
        });
        return attributes;
    }

    public int getFactoryId() {

        return factoryId;
    }

    @Override
    public CustomPortable create(final int i) {

        return new CustomPortable(this, i, this.slots.get(i));
    }

    public CustomPortable create(final Schema schema) {

        return create(schema.getSlot() + SLOT_OFFSET);
    }

    public CustomPortable createRef() {

        return create(REF_SLOT);
    }

    public ClassDefinition def(final int slot) {

        return def(slot, slots.get(slot));
    }

    public ClassDefinition refDef() {

        return def(REF_SLOT);
    }

    public ClassDefinition def(final Schema schema) {

        return def(schema.getSlot() + SLOT_OFFSET);
    }

    public SerializationConfig serializationConfig() {

        return new SerializationConfig()
                .setPortableFactories(Collections.singletonMap(getFactoryId(), this))
                .setClassDefinitions(defs());
    }
}

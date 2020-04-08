package io.basestar.storage.hazelcast.serde;

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

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomPortable implements Portable {

    private final PortableSchemaFactory factory;

    private final int classId;

    private final Map<String, AttributeType<?>> attributes;

    private final Map<String, Object> data;

    public CustomPortable(final PortableSchemaFactory factory, final int classId,
                          final Map<String, AttributeType<?>> attributes) {

        this.factory = factory;
        this.classId = classId;
        this.attributes = attributes;
        this.data = new HashMap<>();
    }

    @Override
    public int getFactoryId() {

        return factory.getFactoryId();
    }

    @Override
    public int getClassId() {

        return classId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writePortable(final PortableWriter writer) throws IOException {

        for(final Map.Entry<String, AttributeType<?>> entry : attributes.entrySet()) {
            final String name = entry.getKey();
            final AttributeType<Object> attr = (AttributeType<Object>)entry.getValue();
            final Object value = data.get(name);
            attr.write(factory, writer, name, value);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readPortable(final PortableReader reader) throws IOException {

        for(final Map.Entry<String, AttributeType<?>> entry : attributes.entrySet()) {
            final String name = entry.getKey();
            final AttributeType<Object> attr = (AttributeType<Object>)entry.getValue();
            final Object value = attr.read(reader, name);
            data.put(name, value);
        }
    }

    public Map<String, Object> getData() {

        return data;
    }

    public void setData(final Map<String, Object> data) {

        this.data.putAll(data);
    }
}

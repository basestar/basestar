package io.basestar.storage.hazelcast.serde;

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

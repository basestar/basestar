package io.basestar.mapper.internal;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Property;
import io.basestar.type.PropertyContext;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class PropertyMapper implements MemberMapper<InstanceSchema.Builder> {

    private final String name;

    private final PropertyContext property;

    private final TypeMapper type;

    public PropertyMapper(final String name, final PropertyContext property) {

        this.name = name;
        this.property = property;
        this.type = TypeMapper.from(property.type());
    }

    @Override
    public void addToSchema(final InstanceSchema.Builder builder) {

        builder.setProperty(name, Property.builder()
                .setType(type.use()));
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

        if(property.canGet()) {
            final Object value = property.get(source);
            target.put(name, type.unmarshall(value, Object.class));
        }
    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

        if(property.canSet()) {
            final Object value = source.get(name);
            property.set(target, type.marshall(value));
        }
    }
}

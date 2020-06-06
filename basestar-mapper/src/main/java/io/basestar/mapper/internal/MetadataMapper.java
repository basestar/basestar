package io.basestar.mapper.internal;

import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.type.PropertyContext;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.Map;

public class MetadataMapper implements MemberMapper<ObjectSchema.Builder> {

    private final Name name;

    private final PropertyContext property;

    private final TypeMapper type;

    public MetadataMapper(final Name name, final PropertyContext property) {

        this.name = name;
        this.property = property;
        this.type = TypeMapper.from(property.type());
    }

    @Override
    public void addToSchema(final ObjectSchema.Builder builder) {

        // no-op
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

        if(property.canGet()) {
            final Object value = property.get(source);
            name.unmarshall(type, target, value);
        }
    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

        if(property.canSet()) {
            final Object value = name.marshall(type, source);
            property.set(target, value);
        }
    }

    public enum Name {

        CREATED {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getCreated(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setCreated(target, type.unmarshall(value, LocalDateTime.class));
            }
        },
        UPDATED {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getUpdated(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setUpdated(target, type.unmarshall(value, LocalDateTime.class));
            }
        },
        HASH {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getHash(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setHash(target, type.unmarshall(value, String.class));
            }
        },
        VERSION {
            @Override
            public Object marshall(final TypeMapper type, final Map<String, Object> source) {

                return type.marshall(Instance.getVersion(source));
            }

            @Override
            public void unmarshall(final TypeMapper type, final Map<String, Object> target, final Object value) {

                Instance.setVersion(target, type.unmarshall(value, Long.class));
            }
        };

        public abstract Object marshall(TypeMapper type, Map<String, Object> source);

        public abstract void unmarshall(TypeMapper type, Map<String, Object> target, Object value);
    }
}

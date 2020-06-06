package io.basestar.mapper.internal;

import io.basestar.schema.ObjectSchema;
import io.basestar.type.TypeContext;

public class ObjectSchemaMapper<T> extends InstanceSchemaMapper<T, ObjectSchema.Builder> {

    public ObjectSchemaMapper(final String name, final TypeContext type) {

        super(name, type, ObjectSchema.Builder.class);
    }

    @Override
    public ObjectSchema.Builder schema() {

        return addMembers(ObjectSchema.builder());
    }
}

package io.basestar.mapper.internal;

import io.basestar.schema.StructSchema;
import io.basestar.type.TypeContext;

public class StructSchemaMapper<T> extends InstanceSchemaMapper<T, StructSchema.Builder> {

    public StructSchemaMapper(final String name, final TypeContext type) {

        super(name, type, StructSchema.Builder.class);
    }

    @Override
    public StructSchema.Builder schema() {

        return addMembers(StructSchema.builder());
    }
}

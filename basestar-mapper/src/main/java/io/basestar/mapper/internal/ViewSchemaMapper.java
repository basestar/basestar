package io.basestar.mapper.internal;

import io.basestar.mapper.MappingContext;
import io.basestar.schema.ViewSchema;
import io.basestar.type.TypeContext;

public class ViewSchemaMapper<T> extends InstanceSchemaMapper<T, ViewSchema.Builder> {

    public ViewSchemaMapper(final MappingContext context, final String name, final TypeContext type) {

        super(context, name, type, ViewSchema.Builder.class);
    }

    @Override
    public ViewSchema.Builder schema() {

        return addMembers(ViewSchema.builder());
    }
}

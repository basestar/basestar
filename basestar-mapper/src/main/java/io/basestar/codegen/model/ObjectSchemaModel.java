package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.StructSchema;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ObjectSchemaModel extends InstanceSchemaModel {

    private final ObjectSchema schema;

    public ObjectSchemaModel(final CodegenSettings settings, final ObjectSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    @Override
    protected Class<?> getAnnotationClass() {

        return io.basestar.mapper.annotation.ObjectSchema.class;
    }

    @Override
    public InstanceSchemaModel getExtend() {

        final InstanceSchema extend = schema.getExtend();
        if(extend instanceof ObjectSchema) {
            return new ObjectSchemaModel(getSettings(), (ObjectSchema) extend);
        } else if(extend instanceof StructSchema) {
            return new StructSchemaModel(getSettings(), (StructSchema) extend);
        } else {
            return null;
        }
    }

    @Override
    public List<MemberModel> getMembers() {

        return Stream.concat(
                super.getMembers().stream(),
                schema.getDeclaredLinks().values().stream()
                        .map(v -> new LinkModel(getSettings(), v))
        ).collect(Collectors.toList());
    }
}
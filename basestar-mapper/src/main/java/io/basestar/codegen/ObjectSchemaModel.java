package io.basestar.codegen;

import io.basestar.schema.ObjectSchema;

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
    public List<MemberModel> getMembers() {

        return Stream.concat(
                super.getMembers().stream(),
                schema.getLinks().values().stream()
                        .map(v -> new LinkModel(getSettings(), v))
        ).collect(Collectors.toList());
    }
}
package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Reserved;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public abstract class InstanceSchemaModel extends SchemaModel {

    private final InstanceSchema schema;

    public InstanceSchemaModel(final CodegenSettings settings, final InstanceSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    public List<MemberModel> getMembers() {

        return Stream.concat(
                schema.getExtend() != null ? Stream.<MemberModel>empty() : schema.metadataSchema().entrySet().stream()
                        .filter(entry -> !Reserved.SCHEMA.equals(entry.getKey()))
                        .map(entry -> new MetadataModel(getSettings(), entry.getKey(), entry.getValue())),
                schema.getDeclaredProperties().values().stream()
                        .map(v -> new PropertyModel(getSettings(), v))
        ).collect(Collectors.toList());
    }

    public abstract InstanceSchemaModel getExtend();
}
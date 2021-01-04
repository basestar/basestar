package io.basestar.codegen.model;

import io.basestar.codegen.CodegenContext;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class LinkableSchemaModel extends InstanceSchemaModel {

    private final LinkableSchema schema;

    public LinkableSchemaModel(final CodegenContext context, final LinkableSchema schema) {

        super(context, schema);
        this.schema = schema;
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        final List<AnnotationModel<?>> annotations = new ArrayList<>(super.getAnnotations());
        final List<Bucketing> bucketing = schema.getDeclaredBucketing();
        if(!bucketing.isEmpty()) {
            annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.Bucketing.Modifier.annotation(bucketing)));
        }
        return annotations;
    }

    @Override
    public List<MemberModel> getAdditionalMembers() {

        return Stream.concat(
                schema.metadataSchema().entrySet().stream()
                        .filter(entry -> !ObjectSchema.SCHEMA.equals(entry.getKey()) && !entry.getKey().startsWith(Reserved.PREFIX))
                        .map(entry -> new MetadataModel(getContext(), entry.getKey(), entry.getValue())),
                schema.getDeclaredLinks().values().stream()
                        .map(v -> new LinkModel(getContext(), v))).collect(Collectors.toList());
    }
}

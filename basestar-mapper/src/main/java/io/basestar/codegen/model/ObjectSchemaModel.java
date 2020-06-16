package io.basestar.codegen.model;

import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.StructSchema;

import java.util.ArrayList;
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
    public List<AnnotationModel> getAnnotations() {

        final List<AnnotationModel> annotations = new ArrayList<>();
        annotations.add(new AnnotationModel(getSettings(), javax.validation.Valid.class));
        annotations.add(new AnnotationModel(getSettings(), io.basestar.mapper.annotation.ObjectSchema.class, ImmutableMap.of("name", schema.getName())));
//        schema.getIndexes().forEach((name, index) -> {
//            final List<String> partition = index.getPartition().stream().map(AbstractPath::toString).collect(Collectors.toList());
//            final List<String> sort = index.getSort().stream().map(Sort::toString).collect(Collectors.toList());
//            final Map<String, Object> values = new HashMap<>();
//            if(!partition.isEmpty()) {
//                values.put("partition", partition);
//            }
//            if(!sort.isEmpty()) {
//                values.put("sort", sort);
//            }
//            annotations.add(new AnnotationModel(getSettings(), io.basestar.mapper.annotation.Index.class, values));
//        });
        return annotations;
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
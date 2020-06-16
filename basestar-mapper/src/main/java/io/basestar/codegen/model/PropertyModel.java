package io.basestar.codegen.model;

import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Property;

import java.util.ArrayList;
import java.util.List;

public class PropertyModel extends MemberModel {

    private final Property property;

    public PropertyModel(final CodegenSettings settings, final Property property) {

        super(settings);

        this.property = property;
    }

    @Override
    public String getName() {

        return property.getName();
    }

    @Override
    public List<AnnotationModel> getAnnotations() {

        final List<AnnotationModel> annotations = new ArrayList<>();
        annotations.add(new AnnotationModel(getSettings(), io.basestar.mapper.annotation.Property.class, ImmutableMap.of("name", property.getName())));
        annotations.add(new AnnotationModel(getSettings(), javax.validation.constraints.NotNull.class, ImmutableMap.of()));
        return annotations;
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getSettings(), property.getType());
    }

    @Override
    public boolean isRequired() {

        return property.isRequired();
    }
}

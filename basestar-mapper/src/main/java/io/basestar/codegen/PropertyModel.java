package io.basestar.codegen;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Property;

import java.util.Map;

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
    protected Class<?> getAnnotationClass() {

        return io.basestar.mapper.annotation.Property.class;
    }

    @Override
    public Map<String, Object> getAnnotationValues() {

        return ImmutableMap.of("name", property.getName());
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

package io.basestar.codegen;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Link;

import java.util.Map;

public class LinkModel extends MemberModel {

    private final Link link;

    public LinkModel(final CodegenSettings settings, final Link link) {

        super(settings);

        this.link = link;
    }

    @Override
    public String getName() {

        return link.getName();
    }

    @Override
    protected Class<?> getAnnotationClass() {

        return io.basestar.mapper.annotation.Link.class;
    }

    @Override
    public Map<String, Object> getAnnotationValues() {

        return ImmutableMap.of(
                "name", link.getName(),
                "expression", link.getExpression()
        );
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getSettings(), link.getType());
    }

    @Override
    public boolean isRequired() {

        return false;
    }
}

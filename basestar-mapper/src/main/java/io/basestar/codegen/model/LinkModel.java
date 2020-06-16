package io.basestar.codegen.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Link;

import java.util.List;

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
    public List<AnnotationModel> getAnnotations() {

        return ImmutableList.of(
                new AnnotationModel(getSettings(), io.basestar.mapper.annotation.Link.class, ImmutableMap.of(
                        "name", link.getName(),
                        "expression", link.getExpression()
                ))
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

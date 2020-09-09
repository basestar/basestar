package io.basestar.schema.layout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class JsonLayout implements Layout.Simple {

    @RequiredArgsConstructor
    public enum Enable {

        ARRAY(Collections.emptySet()),
        SET(Collections.emptySet()),
        MAP(Collections.emptySet()),
        STRUCT(Collections.emptySet()),
        ANY(Collections.emptySet()),
        COLLECTION(ImmutableSet.of(ARRAY, SET)),
        CONTAINER(ImmutableSet.of(ARRAY, SET, MAP));

        @Getter
        private final Set<Enable> implies;
    }

    @Getter
    private final Layout baseLayout;

    private final Set<Enable> enabled;

    private final ObjectMapper objectMapper;

    public JsonLayout(final Layout baseLayout, final Enable ... enabled) {

        this(baseLayout, Arrays.asList(enabled));
    }

    public JsonLayout(final Layout baseLayout, final Collection<? extends Enable> enabled) {

        this(baseLayout, new ObjectMapper(), enabled);
    }

    public JsonLayout(final Layout baseLayout, final ObjectMapper objectMapper, final Enable ... enabled) {

        this(baseLayout, objectMapper, Arrays.asList(enabled));
    }

    public JsonLayout(final Layout baseLayout, final ObjectMapper objectMapper, final Collection<? extends Enable> enabled) {

        this.baseLayout = baseLayout;
        this.objectMapper = objectMapper;
        this.enabled = ImmutableSet.copyOf(enabled);
    }

    private boolean isJsonEnabled(final Enable enable) {

        return enabled.contains(enable) || enabled.stream().anyMatch(v -> v.getImplies().contains(enable));
    }

    @Override
    public Use<?> layoutSchema(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor.Transforming() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Use<?> transform(final Use<?> type, final Set<Name> expand) {

                return layoutSchema(type, expand);
            }

            @Override
            public <T> Use<?> visitArray(final UseArray<T> type) {

                if(isJsonEnabled(Enable.ARRAY)) {
                    return UseString.DEFAULT;
                } else {
                    return Use.Visitor.Transforming.super.visitArray(type);
                }
            }

            @Override
            public <T> Use<?> visitSet(final UseSet<T> type) {

                if(isJsonEnabled(Enable.SET)) {
                    return UseString.DEFAULT;
                } else {
                    return Use.Visitor.Transforming.super.visitSet(type);
                }
            }

            @Override
            public <T> Use<?> visitMap(final UseMap<T> type) {

                if(isJsonEnabled(Enable.MAP)) {
                    return UseString.DEFAULT;
                } else {
                    return Use.Visitor.Transforming.super.visitMap(type);
                }
            }

            @Override
            public Use<?> visitStruct(final UseStruct type) {

                if(isJsonEnabled(Enable.STRUCT)) {
                    return UseString.DEFAULT;
                } else {
                    return Use.Visitor.Transforming.super.visitStruct(type);
                }
            }

            @Override
            public Use<?> visitAny(final UseAny type) {

                if(isJsonEnabled(Enable.ANY)) {
                    return UseString.DEFAULT;
                } else {
                    return Use.Visitor.Transforming.super.visitAny(type);
                }
            }
        });
    }

    @Override
    public Object applyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.TransformingValue() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Object getValue() {

                return value;
            }

            @Override
            public Object transform(final Use<?> type, final Set<Name> expand, final Object value) {

                return applyLayout(type, expand, value);
            }

            @Override
            public <T> Object visitArray(final UseArray<T> type) {

                if(isJsonEnabled(Enable.ARRAY)) {
                    return toJson(value);
                } else {
                    return Use.Visitor.TransformingValue.super.visitArray(type);
                }
            }

            @Override
            public <T> Object visitSet(final UseSet<T> type) {

                if(isJsonEnabled(Enable.SET)) {
                    return toJson(value);
                } else {
                    return Use.Visitor.TransformingValue.super.visitSet(type);
                }
            }

            @Override
            public <T> Object visitMap(final UseMap<T> type) {

                if(isJsonEnabled(Enable.MAP)) {
                    return toJson(value);
                } else {
                    return Use.Visitor.TransformingValue.super.visitMap(type);
                }
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                if(isJsonEnabled(Enable.ANY)) {
                    return toJson(value);
                } else {
                    return Use.Visitor.TransformingValue.super.visitStruct(type);
                }
            }

            @Override
            public Object visitAny(final UseAny type) {

                if(isJsonEnabled(Enable.ANY)) {
                    return toJson(value);
                } else {
                    return Use.Visitor.TransformingValue.super.visitAny(type);
                }
            }
        });
    }

    private String toJson(final Object value) {

        try {
            return objectMapper.writeValueAsString(value);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private <T> T fromJson(final Object value, final Use<T> type) {

        try {
            if(value instanceof String) {
                return type.create(objectMapper.readValue((String)value, Object.class));
            } else {
                throw new UnexpectedTypeException(type, value);
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Object unapplyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.TransformingValue() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Object getValue() {

                return value;
            }

            @Override
            public Object transform(final Use<?> type, final Set<Name> expand, final Object value) {

                return applyLayout(type, expand, value);
            }

            @Override
            public <T> Object visitArray(final UseArray<T> type) {

                if(isJsonEnabled(Enable.ARRAY)) {
                    return fromJson(value, type);
                } else {
                    return Use.Visitor.TransformingValue.super.visitArray(type);
                }
            }

            @Override
            public <T> Object visitSet(final UseSet<T> type) {

                if(isJsonEnabled(Enable.SET)) {
                    return fromJson(value, type);
                } else {
                    return Use.Visitor.TransformingValue.super.visitSet(type);
                }
            }

            @Override
            public <T> Object visitMap(final UseMap<T> type) {

                if(isJsonEnabled(Enable.MAP)) {
                    return fromJson(value, type);
                } else {
                    return Use.Visitor.TransformingValue.super.visitMap(type);
                }
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                if(isJsonEnabled(Enable.ANY)) {
                    return fromJson(value, type);
                } else {
                    return Use.Visitor.TransformingValue.super.visitStruct(type);
                }
            }

            @Override
            public Object visitAny(final UseAny type) {

                if(isJsonEnabled(Enable.ANY)) {
                    return fromJson(value, type);
                } else {
                    return Use.Visitor.TransformingValue.super.visitAny(type);
                }
            }
        });
    }

}

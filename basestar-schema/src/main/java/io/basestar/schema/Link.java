package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseArray;
import io.basestar.schema.use.Widening;
import io.basestar.schema.util.Expander;
import io.basestar.util.*;
import io.leangen.geantyref.TypeFactory;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Link
 */

@Getter
@Accessors(chain = true)
public class Link implements Member {

    @Nonnull
    private final Name qualifiedName;

    @Nullable
    private final String description;

    @Nonnull
    private final LinkableSchema schema;

    @Nonnull
    private final Expression expression;

    @Nonnull
    private final boolean single;

    @Nonnull
    private final List<Sort> sort;

    @Nullable
    private final Visibility visibility;

    @Nonnull
    private final Map<String, Serializable> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Member.Descriptor {

        Name getSchema();

        Expression getExpression();

        Boolean getSingle();

        List<Sort> getSort();

        interface Self extends Member.Descriptor.Self<Link>, Descriptor {

            @Override
            default Name getSchema() {

                return self().getSchema().getQualifiedName();
            }

            @Override
            default Expression getExpression() {

                return self().getExpression();
            }

            @Override
            default Boolean getSingle() {

                return self().isSingle();
            }

            @Override
            default List<Sort> getSort() {

                return self().getSort();
            }

            @Override
            default Visibility getVisibility() {

                return self().getVisibility();
            }
        }

        default Link build(final Schema.Resolver resolver, final Name qualifiedName) {

            return new Link(this, resolver, qualifiedName);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor, Member.Builder<Builder> {

        @Nullable
        private Name schema;

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression expression;

        @Nullable
        private Boolean single;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private Visibility visibility;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Serializable> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private Link(final Descriptor descriptor, final Schema.Resolver resolver, final Name qualifiedName) {

        this.qualifiedName = qualifiedName;
        this.description = descriptor.getDescription();
        this.schema = resolver.requireLinkableSchema(descriptor.getSchema());
        this.expression = Nullsafe.require(descriptor.getExpression());
        this.single = Nullsafe.orDefault(descriptor.getSingle());
        this.sort = Immutable.copy(descriptor.getSort());
        this.visibility = descriptor.getVisibility();
        this.extensions = Immutable.sortedCopy(descriptor.getExtensions());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
    }

    @Override
    public boolean supportsTrivialJoin(final Set<Name> expand) {

        return single;
    }

    @Override
    public boolean canModify(final Member member, final Widening widening) {

        return true;
    }

    @Override
    public boolean canCreate() {

        return true;
    }

    @Override
    public Use<?> getType() {

        if(single) {
            return schema.typeOf();
        } else {
            return new UseArray<>(schema.typeOf());
        }
    }

    @Override
    public Type javaType(final Name name) {

        if(single) {
            return schema.javaType(name);
        } else {
            return TypeFactory.parameterizedClass(List.class, schema.javaType(name));
        }
    }

    @Override
    public Optional<Use<?>> layout(final Set<Name> expand) {

        if(expand == null) {
            return Optional.empty();
        } else {
            return Optional.of(getType());
        }
    }

    public List<Sort> getEffectiveSort() {

        if(sort.isEmpty()) {
            return ImmutableList.of(Sort.asc(schema.id()));
        } else {
            final Sort last = sort.get(sort.size() - 1);
            if(last.getName().equals(Name.of(schema.id()))) {
                return sort;
            } else {
                return ImmutableList.<Sort>builder().addAll(sort)
                        .add(Sort.asc(schema.id()))
                        .build();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Page<Instance> toArray(final Object value) {

        if(single) {
            return value == null ? Page.empty() : Page.single((Instance) value);
        } else if(value == null) {
            return null;
        } else if(value instanceof Page) {
            return (Page<Instance>)value;
        } else if(value instanceof List) {
            return new Page<>((List<Instance>)value, null);
        } else {
            throw new IllegalStateException();
        }
    }

    private Object fromArray(final Page<Instance> value) {

        if(single) {
            return value == null || value.isEmpty() ? null : value.get(0);
        } else {
            return value;
        }
    }

    @Override
    public Object expand(final Object value, final Expander expander, final Set<Name> expand) {

        if(expand == null) {
            return null;
        } else {
            return fromArray(expander.expandLink(this, toArray(value), expand));
        }
    }

    @Override
    public Object create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(single) {
            return schema.create(value, expand, suppress);
        } else {
            return ((Collection<?>)value).stream()
                    .map(v -> schema.create(v, expand, suppress))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public Set<Name> requiredExpand(final Set<Name> names) {

        final Set<Name> result = new HashSet<>();
        result.add(Name.empty());
        result.addAll(schema.requiredExpand(names));
        return result;
    }

    //FIXME
    @Override
    @SuppressWarnings("unchecked")
    public <T> Use<T> typeOf(final Name name) {

        return (Use<T>)getType().typeOf(name);
    }

    @Override
    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        return schema.transientExpand(name, expand);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object applyVisibility(final Context context, final Object value) {

        return transform(value, before -> schema.applyVisibility(context, before));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluateTransients(final Context context, final Object value, final Set<Name> expand) {

        return transform(value, before -> schema.evaluateTransients(context, before, expand));
    }

    @Override
    public Set<Expression> refQueries(Name otherSchemaName, final Set<Name> expand, final Name name) {

        // FIXME
        return Collections.emptySet();
    }

    @Override
    public Set<Name> refExpand(Name otherSchemaName, final Set<Name> expand) {

        // FIXME
        return Collections.emptySet();
    }

    private Object transform(final Object value, final Function<Instance, Instance> fn) {

        return fromArray(transform(toArray(value), fn));
    }

    private Page<Instance> transform(final Page<Instance> value, final Function<Instance, Instance> fn) {

        if(value == null) {
            return null;
        } else {
            boolean changed = false;
            final List<Instance> results = new ArrayList<>();
            for(final Instance before : value) {
                final Instance after = fn.apply(before);
                results.add(after);
                changed = changed || after != before;
            }
            if(changed) {
                return new Page<>(results, value.getPaging(), value.getStats());
            } else {
                return value;
            }
        }
    }

    public Link extend(final Link ext) {

        return ext;
    }

    public static SortedMap<String, Link> extend(final Map<String, Link> base, final Map<String, Link> ext) {

        return Immutable.sortedMerge(base, ext, Link::extend);
    }

    public static SortedMap<String, Link> extend(final Collection<? extends Resolver> base, final Map<String, Link> ext) {

        return Immutable.sortedCopy(Stream.concat(
                base.stream().map(Resolver::getLinks),
                Stream.of(ext)
        ).reduce(Link::extend).orElse(Collections.emptyMap()));
    }

    public interface Resolver {

        interface Descriptor {

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Map<String, Link.Descriptor> getLinks();
        }

        interface Builder<B extends Builder<B>> extends Descriptor {

            default B setLink(final String name, final Link.Descriptor v) {

                return setLinks(Immutable.copyPut(getLinks(), name, v));
            }

            B setLinks(Map<String, Link.Descriptor> vs);
        }

        Map<String, Link> getDeclaredLinks();

        Map<String, Link> getLinks();

        default Map<String, Link.Descriptor> describeDeclaredLinks() {

            return getDeclaredLinks().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().descriptor()
            ));
        }

        default Link getLink(final String name, final boolean inherited) {

            if(inherited) {
                return getLinks().get(name);
            } else {
                return getDeclaredLinks().get(name);
            }
        }

        default Link requireLink(final String name, final boolean inherited) {

            final Link result = getLink(name, inherited);
            if (result == null) {
                throw new MissingMemberException(name);
            } else {
                return result;
            }
        }
    }

    @Override
    public Descriptor descriptor() {

        return (Descriptor.Self) () -> Link.this;
    }
}

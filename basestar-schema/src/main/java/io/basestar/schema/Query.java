package io.basestar.schema;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.schema.exception.MissingQueryException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UsePage;
import io.basestar.schema.util.Cascade;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.ValueContext;
import io.basestar.schema.util.Widening;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class Query implements Member {

    @Nonnull
    private final LinkableSchema schema;

    @Nonnull
    @JsonIgnore
    private final Name qualifiedName;

    @Nullable
    private final String description;

    @Nonnull
    private final List<Argument> arguments;

    @Nullable
    private final Expression expression;

    @Nonnull
    private final List<Sort> sort;

    @Nullable
    private final Visibility visibility;

    @Nonnull
    private final Map<String, Serializable> extensions;

    @JsonDeserialize(as = Query.Builder.class)
    public interface Descriptor extends Member.Descriptor {

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Argument.Descriptor> getArguments();

        Expression getExpression();

        Visibility getVisibility();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Sort> getSort();

        interface Self extends Query.Descriptor, Member.Descriptor.Self<Query> {

            @Override
            default List<Argument.Descriptor> getArguments() {

                return Immutable.transform(self().getArguments(), Argument::descriptor);
            }

            @Override
            default Expression getExpression() {

                return self().getExpression();
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

        default Query build(final Schema.Resolver.Constructing resolver, final LinkableSchema schema, final Name qualifiedName) {

            return new Query(this, resolver, schema, qualifiedName);
        }
    }

    public interface Resolver {

        interface Descriptor {

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Map<String, Query.Descriptor> getQueries();
        }

        interface Builder<B extends Resolver.Builder<B>> extends Resolver.Descriptor {

            default B setQuery(final String name, final Query.Descriptor v) {

                return setQueries(Immutable.put(getQueries(), name, v));
            }

            B setQueries(Map<String, Query.Descriptor> vs);
        }

        Map<String, Query> getDeclaredQueries();

        Map<String, Query> getQueries();

        default Map<String, Query.Descriptor> describeDeclaredQueries() {

            return getDeclaredQueries().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().descriptor()
            ));
        }

        default Query getQuery(final String name, final boolean inherited) {

            if (inherited) {
                return getQueries().get(name);
            } else {
                return getDeclaredQueries().get(name);
            }
        }

        default Query requireQuery(final String name, final boolean inherited) {

            final Query result = getQuery(name, inherited);
            if (result == null) {
                throw new MissingQueryException(name);
            } else {
                return result;
            }
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonPropertyOrder({"description", "expression", "visibility", "extensions"})
    public static class Builder implements Descriptor, Member.Builder<Builder> {

        private String description;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<Argument.Descriptor> arguments;

        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression expression;

        private Visibility visibility;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        @Nullable
        private Map<String, Serializable> extensions;

        @JsonCreator
        @SuppressWarnings("unused")
        public static Query.Builder fromExpression(final String expression) {

            return new Query.Builder()
                    .setExpression(Expression.parse(expression));
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public Query(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final LinkableSchema schema, final Name qualifiedName) {

        this.schema = schema;
        this.arguments = Immutable.transform(descriptor.getArguments(), v -> v.build(resolver));
        this.qualifiedName = qualifiedName;
        this.description = descriptor.getDescription();
        this.expression = descriptor.getExpression();
        this.visibility = descriptor.getVisibility();
        this.sort = descriptor.getSort();
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
    }

    @Override
    public boolean supportsTrivialJoin(final Set<Name> expand) {

        return false;
    }

    @Override
    public boolean requiresMigration(final Member member, final Widening widening) {

        return false;
    }

    @Override
    public Use<?> typeOf() {

        return new UsePage<>(schema.typeOf());
    }

    @Override
    public Optional<Use<?>> layout(final Set<Name> expand) {

        return Optional.of(typeOf());
    }

    @Override
    public void expand(final Name parent, final Expander expander, final Set<Name> expand) {

    }

    @Override
    public Object expand(final Name parent, final Object value, final Expander expander, final Set<Name> expand) {

        return value;
    }

    @Override
    public Set<Name> requiredExpand(final Set<Name> names) {

        return Immutable.set();
    }

    @Override
    public <T> Optional<Use<T>> optionalTypeOf(final Name name) {

        return Optional.empty();
    }

    @Override
    public Type javaType(final Name name) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        return Immutable.set();
    }

    @Override
    public Object applyVisibility(final Context context, final Object value) {

        return value;
    }

    @Override
    public Object evaluateTransients(final Context context, final Object value, final Set<Name> expand) {

        return value;
    }

    @Override
    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        return Immutable.set();
    }

    @Override
    public Set<Expression> cascadeQueries(final Cascade cascade, final Name otherSchemaName, final Name name) {

        return Immutable.set();
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        return Immutable.set();
    }

    @Override
    public Object create(final ValueContext context, final Object value, final Set<Name> expand) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Query.Descriptor descriptor() {

        return (Query.Descriptor.Self) () -> Query.this;
    }

    public Query extend(final Query ext) {

        return ext;
    }

    public static SortedMap<String, Query> extend(final Map<String, Query> base, final Map<String, Query> ext) {

        return Immutable.sortedMerge(base, ext, Query::extend);
    }

    public static SortedMap<String, Query> extend(final Collection<? extends Query.Resolver> base, final Map<String, Query> ext) {

        return Immutable.sortedMap(Stream.concat(
                base.stream().map(Query.Resolver::getQueries),
                Stream.of(ext)
        ).reduce(Query::extend).orElse(Collections.emptyMap()));
    }
}

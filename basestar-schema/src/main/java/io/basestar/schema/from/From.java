package io.basestar.schema.from;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.AbbrevSetDeserializer;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.SchemaRef;
import io.basestar.util.*;
import lombok.Data;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*

# referenced schema

from:
    schema: X

# inline schema

from:
    schema:
        type: object

# union

from:
    union:
        - schema: X
        - schema: Y

# join

from:
    join:
        left:
            schema: X
            as: x
        right:
            schema: Y
            as: x
        type: inner

 */

public interface From extends Serializable {

    From EXTERNAL = new FromExternal();

//    String getAs();
//
//    List<Sort> getSort();
//
//    Map<String, Expression> getSelect();
//
//    Expression getWhere();
//
//    List<String> getGroup();

    Descriptor descriptor();

    default From as(final String as) {

        return new FromAlias(this, as);
    }

    default From map(final Map<String, Expression> map) {

        return new FromMap(this, map);
    }

    default From filter(final Expression where) {

        return new FromFilter(this, where);
    }

    default From agg(final List<String> group, final Map<String, Expression> agg) {

        return new FromAgg(this, group, agg);
    }

    default From join(final From right, final Join.Type type, final Expression on) {

        return new FromJoin(new Join(this, right, type, on));
    }


    default From select(final Map<String, Expression> select) {

        if(select.values().stream().anyMatch(Expression::isAggregate)) {
            return agg(Immutable.list(), select);
        } else {
            return map(select);
        }
    }

    default From sort(final List<Sort> order) {

        return new FromSort(this, order);
    }

    InferenceContext inferenceContext();

    void collectMaterializationDependencies(Map<Name, LinkableSchema> out);

    void collectDependencies(Map<Name, Schema<?>> out);

    Expression id();

    Use<?> typeOfId();

    Map<String, Use<?>> getProperties();

    BinaryKey id(Map<String, Object> row);

    boolean isCompatibleBucketing(List<Bucketing> other);

    List<FromSchema> schemas();

    default boolean hasAlias() {

        return false;
    }

    default String digest() {

        return "" + System.identityHashCode(this);
    }

    default String getAlias() {

        return "_anon_" + digest();
    }

    boolean isExternal();

//    default boolean isGrouping() {
//
//        return !getGroup().isEmpty();
//    }
//
//    default boolean isAggregating() {
//
//        return getSelect().values().stream().anyMatch(Expression::isAggregate);
//    }

//    default boolean hasSelect() {
//
//        return !Nullsafe.orDefault(getSelect()).isEmpty();
//    }
//
//    default boolean hasWhere() {
//
//        return getWhere() != null;
//    }
//
//    default boolean hasSort() {
//
//        return !Nullsafe.orDefault(getSort()).isEmpty();
//    }

    @JsonDeserialize(as = Builder.class)
    interface Descriptor {

        String FROM = "from";

        String SQL = "sql";

        String UNION = "union";

        String JOIN = "join";

        String SCHEMA = "schema";

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Descriptor getFrom();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        SchemaRef getSchema();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<Name> getExpand();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        String getSql();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Descriptor> getUsing();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<String> getPrimaryKey();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        List<Descriptor> getUnion();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getAll();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Join.Descriptor getJoin();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Sort> getOrder();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Map<String, Expression> getSelect();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        List<String> getGroup();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Expression getWhere();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        String getAs();

        default From buildUndecorated(final Schema.Resolver.Constructing resolver, final Context context) {

            final Logger log = LoggerFactory.getLogger(Descriptor.class);

            final Descriptor from = getFrom();
            final String sql = getSql();
            final List<Descriptor> union = getUnion();
            final Join.Descriptor join = getJoin();
            final SchemaRef schema = getSchema();

            final Map<String, Boolean> args = ImmutableMap.of(
                    FROM, from != null,
                    SQL, sql != null,
                    UNION, union != null,
                    JOIN, join != null,
                    SCHEMA, schema != null
            );

            final List<String> names = args.entrySet().stream().filter(Map.Entry::getValue)
                    .map(Map.Entry::getKey).collect(Collectors.toList());

            if(names.size() == 1) {

                if(from != null) {
                    return from.build(resolver, context);
                } else if(sql != null) {
                    final Map<String, From> using = Immutable.transformValues(getUsing(), (k, v) -> v.build(resolver, context));
//                    final FromSqlVisitor visitor = new FromSqlVisitor(resolver, using);
//                    From impl = null;
//                    try {
//                        impl = visitor.visit(Expression.parseAndBind(context, sql));
//                    } catch (final Exception e) {
//                        log.error("Failed to process SQL from clause, leaving as raw SQL", e);
//                    }
                    return new FromSql(sql, getPrimaryKey(), using, null);
                } else if(union != null && !union.isEmpty()) {
                    final List<From> unionFrom = Immutable.transform(getUnion(), v -> v.build(resolver, context));
                    return new FromUnion(unionFrom, Nullsafe.orDefault(getAll()));
                } else if(join != null) {
                    final Join joinFrom = join.build(resolver, context);
                    return new FromJoin(joinFrom);
                } else if(schema != null) {
                    return new FromSchema(schema.resolve(resolver), Nullsafe.orDefault(getExpand()));
                } else {
                    throw new UnsupportedOperationException();
                }

            } else if(names.size() == 0) {
                return EXTERNAL;
            } else {
                throw new IllegalStateException("At most one of " + args.keySet() + " is supported (provided: " + names + ")");
            }
        }

        default From build(final Schema.Resolver.Constructing resolver, final Context context) {

            From result = buildUndecorated(resolver, context);
            final Expression where = getWhere();
            if(where != null) {
                result = result.filter(where.bind(context));
            }
            final List<String> group = getGroup();
            final Map<String, Expression> select = getSelect();
            if(group != null && !group.isEmpty()) {
                result = result.agg(group, Immutable.transformValues(select, (k, v) -> v.bind(context)));
            } else if(select != null && !select.isEmpty()) {
                result = result.select(Immutable.transformValues(select, (k, v) -> v.bind(context)));
            }
            final List<Sort> order = getOrder();
            if(order != null && !order.isEmpty()) {
                result = result.sort(order);
            }
            final String as = getAs();
            if(as != null) {
                result = result.as(as);
            }
            return result;
        }

        interface Defaulting extends Descriptor {

            @Override
            default Descriptor getFrom() {

                return null;
            }

            @Override
            default SchemaRef getSchema() {

                return null;
            }

            @Override
            default Set<Name> getExpand() {

                return null;
            }

            @Override
            default String getSql() {

                return null;
            }

            @Override
            default Map<String, Descriptor> getUsing() {

                return null;
            }

            @Override
            default List<String> getPrimaryKey() {

                return null;
            }

            @Override
            default List<Descriptor> getUnion() {

                return null;
            }

            @Override
            default Boolean getAll() {

                return null;
            }

            @Override
            default Join.Descriptor getJoin() {

                return null;
            }

            @Override
            default List<Sort> getOrder() {

                return null;
            }

            @Override
            default Map<String, Expression> getSelect() {

                return null;
            }

            @Override
            default List<String> getGroup() {

                return null;
            }

            @Override
            default Expression getWhere() {

                return null;
            }

            @Override
            default String getAs() {

                return null;
            }
        }

        interface Delegating extends Descriptor {

            Descriptor delegate();

            @Override
            default Descriptor getFrom() {

                return delegate().getFrom();
            }

            @Override
            default SchemaRef getSchema() {

                return delegate().getSchema();
            }

            @Override
            default Set<Name> getExpand() {

                return delegate().getExpand();
            }

            @Override
            default String getSql() {

                return delegate().getSql();
            }

            @Override
            default Map<String, Descriptor> getUsing() {

                return delegate().getUsing();
            }

            @Override
            default List<String> getPrimaryKey() {

                return delegate().getPrimaryKey();
            }

            @Override
            default List<Descriptor> getUnion() {

                return delegate().getUnion();
            }

            @Override
            default Boolean getAll() {

                return delegate().getAll();
            }

            @Override
            default Join.Descriptor getJoin() {

                return delegate().getJoin();
            }

            @Override
            default List<Sort> getOrder() {

                return delegate().getOrder();
            }

            @Override
            default Map<String, Expression> getSelect() {

                return delegate().getSelect();
            }

            @Override
            default List<String> getGroup() {

                return delegate().getGroup();
            }

            @Override
            default Expression getWhere() {

                return delegate().getWhere();
            }

            @Override
            default String getAs() {

                return delegate().getAs();
            }
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"schema", "expand", "sort"})
    class Builder implements Descriptor {

        @Nullable
        private Descriptor from;

        @Nullable
        private SchemaRef schema;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(using = AbbrevSetDeserializer.class)
        private Set<Name> expand;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> order;

        @Nullable
        private String sql;

        @Nullable
        private Map<String, Descriptor> using;

        @Nullable
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<String> primaryKey;

        @Nullable
        private List<Descriptor> union;

        @Nullable
        private Boolean all;

        @Nullable
        private Join.Descriptor join;

        private List<Name> by;

        @Nullable
        private String as;

        @Nullable
        private Map<String, Expression> select;

        @Nullable
        private List<String> group;

        @Nullable
        private Expression where;

        @Nullable
        private Integer limit;

        @Nullable
        private Integer offset;

        @JsonCreator
        @SuppressWarnings("unused")
        public static Builder fromSchema(final String schema) {

            return fromSchema(Name.parse(schema));
        }

        public static Builder fromSchema(final Name schema) {

            return new Builder().setSchema(SchemaRef.withName(schema));
        }

        public Builder setSort(final List<Sort> sort) {

            return setOrder(sort);
        }
    }

    static Builder builder() {

        return new Builder();
    }

    <T> T visit(FromVisitor<T> visitor);
}

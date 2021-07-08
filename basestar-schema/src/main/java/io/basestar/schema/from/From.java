package io.basestar.schema.from;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.AbbrevSetDeserializer;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.SchemaRef;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    String getAs();

    List<Sort> getSort();

    Map<String, Expression> getSelect();

    Expression getWhere();

    List<Name> getGroup();

    Descriptor descriptor();

    From as(String as);

    From select(Map<String, Expression> select);

    From where(Expression where);

    From group(List<Name> group);

    InferenceContext inferenceContext();

    void collectMaterializationDependencies(Map<Name, LinkableSchema> out);

    void collectDependencies(Map<Name, Schema<?>> out);

    Expression id();

    Use<?> typeOfId();

    void validateProperty(Property property);

    BinaryKey id(Map<String, Object> row);

    boolean isCompatibleBucketing(List<Bucketing> other);

    List<FromSchema> schemas();

    default void validateSchema(final ViewSchema schema) {

        schema.getDeclaredProperties().values().forEach(this::validateProperty);
    }

    default boolean isGrouping() {

        return !getGroup().isEmpty();
    }

    default boolean isAggregating() {

        return getSelect().values().stream().anyMatch(Expression::isAggregate);
    }

    @JsonDeserialize(as = Builder.class)
    interface Descriptor {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        default SchemaRef getSchema() {

            return null;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        default Set<Name> getExpand() {

            return null;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        default String getSql() {

            return null;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        default Map<String, Descriptor> getUsing() {

            return null;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        default List<String> getPrimaryKey() {

            return null;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        default List<Descriptor> getUnion() {

            return null;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        default Join.Descriptor getJoin() {

            return null;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Sort> getSort();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Map<String, Expression> getSelect();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Expression getWhere();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        List<Name> getGroup();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        String getAs();

        default From build(final Schema.Resolver.Constructing resolver) {

            if(getSql() != null) {
                final Map<String, From> using = Immutable.transformValues(getUsing(), (k, v) -> v.build(resolver));
                final FromSqlVisitor visitor = new FromSqlVisitor(resolver, using);
                final Context context = Context.init();
                return visitor.visit(Expression.parseAndBind(context, getSql()));
            } else if(getUnion() != null) {
                return new FromUnion(resolver, this);
            } else if(getJoin() != null) {
                return new FromJoin(resolver, this);
            } else {
                return new FromSchema(resolver, this);
            }
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"schema", "expand", "sort"})
    class Builder implements Descriptor {

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
        private List<Sort> sort;

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
        private Join.Descriptor join;

        private List<Name> by;

        @Nullable
        private String as;

        @Nullable
        private Map<String, Expression> select;

        @Nullable
        private Expression where;

        @Nullable
        private List<Name> group;

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
    }

    static Builder builder() {

        return new Builder();
    }
}

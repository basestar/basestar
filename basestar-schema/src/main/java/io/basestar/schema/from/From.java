package io.basestar.schema.from;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.AbbrevSetDeserializer;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Property;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface From extends Serializable {

    Descriptor descriptor();

    InferenceContext inferenceContext();

    void collectMaterializationDependencies(Map<Name, LinkableSchema> out);

    void collectDependencies(Map<Name, Schema<?>> out);

    Use<?> typeOfId();

    void validateProperty(Property property);

    @JsonDeserialize(as = Builder.class)
    interface Descriptor {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Name getSchema();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Sort> getSort();

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

        default From build(final Schema.Resolver.Constructing resolver) {

            if(getSql() != null) {
                return new FromSql(resolver, this);
            } else if(getUnion() != null) {
                return new FromUnion(resolver, this);
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
        private Name schema;

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

        @JsonCreator
        @SuppressWarnings("unused")
        public static Builder fromSchema(final String schema) {

            return fromSchema(Name.parse(schema));
        }

        public static Builder fromSchema(final Name schema) {

            return new Builder().setSchema(schema);
        }
    }

    static Builder builder() {

        return new Builder();
    }
}

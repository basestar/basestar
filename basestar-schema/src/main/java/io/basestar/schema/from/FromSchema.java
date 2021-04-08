package io.basestar.schema.from;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.*;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.SchemaRef;
import io.basestar.util.*;
import lombok.Getter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public class FromSchema implements From {

    @Nonnull
    private final LinkableSchema schema;

    @Nonnull
    private final List<Sort> sort;

    @Nonnull
    private final Set<Name> expand;

    private final String as;

    public FromSchema(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {

        this.schema = from.getSchema().resolve(resolver);
        this.sort = Nullsafe.orDefault(from.getSort());
        this.expand = Nullsafe.orDefault(from.getExpand());
        this.as = from.getAs();
    }

    @Override
    public From.Descriptor descriptor() {

        return new From.Descriptor() {
            @Override
            public SchemaRef getSchema() {

                if(schema.isAnonymous()) {
                    return SchemaRef.withInline(schema.descriptor());
                } else {
                    return SchemaRef.withName(schema.getQualifiedName());
                }
            }

            @Override
            public List<Sort> getSort() {

                return sort;
            }

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public String getAs() {

                return as;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        return InferenceContext.from(getSchema());
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        final LinkableSchema fromSchema = getSchema();
        if (!out.containsKey(fromSchema.getQualifiedName())) {
            out.put(fromSchema.getQualifiedName(), fromSchema);
            fromSchema.collectMaterializationDependencies(getExpand(), out);
        }
    }

    @Override
    public void collectDependencies(final Map<Name, Schema<?>> out) {

        getSchema().collectDependencies(getExpand(), out);
    }

    @Override
    public Use<?> typeOfId() {

        return getSchema().typeOfId();
    }

    @Override
    public void validateProperty(final Property property) {

        if (property.getExpression() == null) {
            throw new SchemaValidationException(property.getQualifiedName(), "Every view property must have an expression");
        }
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        return BinaryKey.from(ImmutableList.of(row.get(schema.id())));
    }

    @Override
    public Expression id() {

        return new NameConstant(schema.id());
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return schema.isCompatibleBucketing(other);
    }

    @Override
    public List<FromSchema> schemas() {

        return ImmutableList.of(this);
    }
}

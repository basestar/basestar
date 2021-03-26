package io.basestar.schema.from;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Property;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;
import lombok.Getter;

import javax.annotation.Nonnull;
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

        this.schema = resolver.requireLinkableSchema(from.getSchema());
        this.sort = Nullsafe.orDefault(from.getSort());
        this.expand = Nullsafe.orDefault(from.getExpand());
        this.as = from.getAs();
    }

    @Override
    public From.Descriptor descriptor() {

        return new From.Descriptor() {
            @Override
            public Name getSchema() {

                return schema.getQualifiedName();
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
}

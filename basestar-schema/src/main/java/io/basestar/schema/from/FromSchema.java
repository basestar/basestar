package io.basestar.schema.from;

import com.google.common.collect.ImmutableList;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.SchemaRef;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class FromSchema implements From {

    @Nonnull
    private final LinkableSchema schema;

    @Nonnull
    private final Set<Name> expand;

    public FromSchema(final LinkableSchema schema, final Set<Name> expand) {

        this.schema = Nullsafe.require(schema);
        this.expand = Immutable.set(expand);
    }

//
//    protected FromSchema(final LinkableSchema schema, final Set<Name> expand, final Arguments arguments) {
//
//        super(arguments);
//        this.schema = Nullsafe.require(schema);
//        this.expand = Nullsafe.orDefault(expand);
//    }
//
//    public FromSchema(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {
//
//        super(from);
//        this.schema = from.getSchema().resolve(resolver);
//        this.expand = Nullsafe.orDefault(from.getExpand());
//    }

    @Override
    public From.Descriptor descriptor() {

        return new Descriptor.Defaulting() {
            @Override
            public SchemaRef getSchema() {

                if(schema.isAnonymous()) {
                    return SchemaRef.withInline(schema.descriptor());
                } else {
                    return SchemaRef.withName(schema.getQualifiedName());
                }
            }

            @Override
            public Set<Name> getExpand() {

                return expand;
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
    public Map<String, Use<?>> getProperties() {

        return schema.layoutSchema(expand);
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

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        return visitor.visitSchema(this);
    }

    @Override
    public boolean isExternal() {

        return false;
    }
}

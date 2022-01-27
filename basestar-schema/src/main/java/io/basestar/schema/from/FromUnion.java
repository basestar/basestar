package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class FromUnion implements From {

    @Nonnull
    private final List<From> union;

    private final boolean all;

    public FromUnion(final List<From> union, final boolean all) {

        this.union = Immutable.list(union);
        this.all = all;
    }

//    protected FromUnion(final List<From> union, final Arguments arguments) {
//
//        super(arguments);
//        this.union = Nullsafe.require(union);
//    }
//
//    public FromUnion(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {
//
//        super(from);
//        this.union = Immutable.transform(from.getUnion(), v -> v.build(resolver));
//    }

//    @Override
//    protected FromUnion with(final Arguments arguments) {
//
//        return new FromUnion(union, arguments);
//    }
//
//    @Override
//    public From.Descriptor descriptor() {
//
//        return new AbstractFrom.Descriptor(getArguments()) {
//
//            @Override
//            public List<From.Descriptor> getUnion() {
//
//                return Immutable.transform(union, From::descriptor);
//            }
//        };
//    }

    @Override
    public Descriptor descriptor() {

        return new Descriptor.Defaulting() {
            @Override
            public List<Descriptor> getUnion() {

                return Immutable.transform(union, From::descriptor);
            }

            @Override
            public Boolean getAll() {

                return all;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        return new InferenceContext.Union(union.stream().map(From::inferenceContext).collect(Collectors.toList()));
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, Schema> out) {

        union.forEach(v -> v.collectMaterializationDependencies(out));
    }

    @Override
    public void collectDependencies(final Map<Name, Schema> out) {

        union.forEach(v -> v.collectDependencies(out));
    }

    @Override
    public Use<?> typeOfId() {

        // FIXME: must validate that all clauses have the same id type
        final From first = union.get(0);
        return first.typeOfId();
    }

    @Override
    public Map<String, Use<?>> getProperties() {
        return null;
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        // FIXME: must validate that all clauses have the same id type
        final From first = union.get(0);
        return first.id(row);
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return union.stream().allMatch(v -> v.isCompatibleBucketing(other));
    }

    @Override
    public List<FromSchema> schemas() {

        return union.stream().flatMap(from -> from.schemas().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Expression id() {

        // FIXME: must validate that all clauses have the same id expression
        final From first = union.get(0);
        return first.id();
    }

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        return visitor.visitUnion(this);
    }

    @Override
    public boolean isExternal() {

        return union.stream().anyMatch(From::isExternal);
    }
}

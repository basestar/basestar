package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.exception.MissingPropertyException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@Deprecated
public class FromSql extends AbstractFrom {

    private final String sql;

    private final List<String> primaryKey;

    private final Map<String, From> using;

    public FromSql(final String sql, final List<String> primaryKey, final Map<String, From> using, final Arguments arguments) {

        super(arguments);
        this.sql = sql;
        this.primaryKey = Immutable.list(primaryKey);
        this.using = Immutable.map(using);
    }

    public FromSql(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {

        super(from);
        this.sql = Nullsafe.require(from.getSql());
        this.primaryKey = Immutable.list(Nullsafe.require(from.getPrimaryKey()));
        this.using = Immutable.transformValuesSorted(from.getUsing(), (k, v) -> v.build(resolver));
    }

    @Override
    protected AbstractFrom with(final Arguments arguments) {

        return new FromSql(sql, primaryKey, using, arguments);
    }

    @Override
    public From.Descriptor descriptor() {

        return new AbstractFrom.Descriptor(getArguments()) {

            @Override
            public String getSql() {

                return sql;
            }

            @Override
            public Map<String, From.Descriptor> getUsing() {

                return Immutable.transformValuesSorted(using, (k, v) -> v.descriptor());
            }

            @Override
            public List<String> getPrimaryKey() {

                return primaryKey;
            }
        };
    }

    @Override
    public InferenceContext undecoratedInferenceContext() {

        return InferenceContext.empty();
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        using.forEach((k, v) -> v.collectMaterializationDependencies(out));
    }

    @Override
    public void collectDependencies(final Map<Name, Schema<?>> out) {

        using.forEach((k, v) -> v.collectDependencies(out));
    }

    @Override
    public Use<?> typeOfId() {

        return UseBinary.DEFAULT;
    }

    @Override
    public void validateSchema(final ViewSchema schema) {

        super.validateSchema(schema);
        primaryKey.forEach(k -> {
            try {
                schema.requireProperty(k, true);
            } catch (final MissingPropertyException e) {
                throw new IllegalStateException("Cannot use " + k + " in primary key (must be defined in the schema)");
            }
        });
    }

    @Override
    public void validateProperty(final Property property) {

        if (property.getExpression() != null) {
            throw new SchemaValidationException(property.getQualifiedName(), "SQL view properties should not have expressions");
        }
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return using.values().stream().allMatch(v -> v.isCompatibleBucketing(other));
    }

    @Override
    public List<FromSchema> schemas() {

        return getUsing().values().stream().flatMap(from -> from.schemas().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Expression id() {

        throw new UnsupportedOperationException();
    }
}

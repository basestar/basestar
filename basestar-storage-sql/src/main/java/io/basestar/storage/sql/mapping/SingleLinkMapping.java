package io.basestar.storage.sql.mapping;

import io.basestar.expression.Expression;
import io.basestar.schema.Reserved;
import io.basestar.storage.sql.resolver.ExpressionResolver;
import io.basestar.storage.sql.resolver.RecordResolver;
import io.basestar.storage.sql.resolver.TableResolver;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.util.*;

@RequiredArgsConstructor
public class SingleLinkMapping implements LinkMapping {

    private final Expression expression;

    private final QueryMapping schema;

    @Override
    public Set<Name> supportedExpand(final Set<Name> expand) {

        final Set<Name> result = new HashSet<>();
        result.add(Name.empty());
        result.addAll(schema.supportedExpand(expand));
        return result;
    }

    @Override
    public Object fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

        final Boolean present = record.get(qualifiedName.with("__"), SQLDataType.BOOLEAN);
        if (present != null && present) {
            final Map<String, Object> merged = new HashMap<>();
            schema.fromRecord(new RecordResolver() {
                @Override
                public <T> T get(final Name name, final DataType<T> targetType) {

                    return record.get(qualifiedName.with("_").with(name), targetType);
                }
            }, expand).forEach((k, v) -> {
                if (!merged.containsKey(k)) {
                    merged.put(k, v);
                }
            });
            return merged;
        } else {
            return null;
        }
    }

    @Override
    public Optional<Field<?>> nestedField(final Name qualifiedName, final Name name) {

        return schema.nestedField(qualifiedName.with("_"), name);
    }

    @Override
    public Table<?> join(final Name qualifiedName, final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Table<?> table, final Set<Name> expand) {

        final Table<?> target = schema.subselect(qualifiedName.with("_"), context, tableResolver, expressionResolver, expand);
        // FIXME: remove use of inference context here
        final Condition condition = expressionResolver.condition(context, tableResolver, name -> {

            if (Reserved.THIS_NAME.isParentOrEqual(name)) {
                return Optional.of(DSL.field(QueryMapping.selectName(qualifiedName.withoutLast().with(name.withoutFirst()))));
            } else {
                return Optional.of(DSL.field(QueryMapping.selectName(qualifiedName.with("_").with(name))));
            }
        }, schema, expression);

        final SelectField<?> presence = DSL.field(DSL.inline(true).as(QueryMapping.selectName(qualifiedName.with("__"))));
        return table.leftJoin(DSL.table(DSL.select(DSL.asterisk(), presence).from(target)).as(QueryMapping.selectName(qualifiedName)))
                .on(condition);
    }
}

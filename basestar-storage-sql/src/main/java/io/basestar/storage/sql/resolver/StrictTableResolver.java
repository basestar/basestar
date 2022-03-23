package io.basestar.storage.sql.resolver;

import io.basestar.schema.Index;
import io.basestar.schema.Schema;
import io.basestar.storage.sql.strategy.NamingStrategy;
import io.basestar.storage.sql.strategy.SQLStrategy;
import io.basestar.util.Name;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.Optional;

public class StrictTableResolver implements TableResolver {

    private final SQLStrategy strategy;

    public StrictTableResolver(final SQLStrategy strategy) {

        this.strategy = strategy;
    }

    @Override
    public Optional<ResolvedTable> table(final DSLContext context, final Schema schema, final Map<String, Object> arguments, final Index index, final boolean versioned) {

        final NamingStrategy namingStrategy = strategy.getNamingStrategy();
        return namingStrategy.entityName(schema, versioned).map(tableName -> {
            final Table<?> table = DSL.table(tableName);
            return new ResolvedTable() {
                @Override
                public Table<?> getTable() {

                    return table;
                }

                @Override
                public Optional<Field<?>> column(final Name name) {

                    return Optional.of(DSL.field(namingStrategy.columnName(name)));
                }
            };
        });
    }
}

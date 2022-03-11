package io.basestar.storage.sql.resolver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.basestar.schema.Index;
import io.basestar.schema.Schema;
import io.basestar.storage.sql.strategy.NamingStrategy;
import io.basestar.storage.sql.strategy.SQLStrategy;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FlexibleTableResolver implements TableResolver {

    public static final Duration TABLE_CACHE_DURATION = Duration.ofMinutes(30);

    private final SQLStrategy strategy;

    private final Cache<Name, Table<?>> tableCache;

    public FlexibleTableResolver(final SQLStrategy strategy) {

        this.strategy = strategy;
        this.tableCache = CacheBuilder.newBuilder().expireAfterAccess(TABLE_CACHE_DURATION).build();
    }

    @Override
    public Optional<ResolvedTable> table(final DSLContext context, final Schema schema, final Map<String, Object> arguments, final Index index, final boolean versioned) {

        final NamingStrategy namingStrategy = strategy.getNamingStrategy();
        return namingStrategy.entityName(schema, versioned).flatMap(tableName -> {

            final Table<?> table = describeTable(context, tableName);
            if (table != null) {
                return Optional.of(new ResolvedTable() {
                    @Override
                    public Table<?> getTable() {

                        return DSL.table(table.getQualifiedName());
                    }

                    @Override
                    public Optional<Field<?>> column(final io.basestar.util.Name name) {

                        final Table<?> table = describeTable(context, tableName);
                        final org.jooq.Name normalizedName = DSL.name(strategy.getNamingStrategy().columnName(name));
                        final List<Name> fields = Arrays.stream(table.fields())
                                .map(Named::getQualifiedName)
                                .map(f -> {
                                    if (f.parts().length > normalizedName.parts().length) {
                                        return DSL.name(Arrays.stream(f.parts()).skip(f.parts().length - normalizedName.parts().length).toArray(org.jooq.Name[]::new));
                                    } else {
                                        return f;
                                    }
                                })
                                .filter(f -> f.equals(normalizedName))
                                .collect(Collectors.toList());
                        if (fields.isEmpty()) {
                            return Optional.empty();
                        } else if (fields.size() > 1) {
                            throw new IllegalStateException("Field " + name + " is ambiguous in " + table.getQualifiedName());
                        } else {
                            return Optional.of(DSL.field(fields.get(0)));
                        }
                    }
                });
            } else {
                return Optional.empty();
            }
        });
    }

    public Table<?> describeTable(final DSLContext context, final org.jooq.Name name) {

        try {
            return tableCache.get(name, () -> strategy.describeTable(context, name));
        } catch (final ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }
}

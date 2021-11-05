package io.basestar.storage.sql.strategy;

import io.basestar.schema.FunctionSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.storage.sql.util.DDLStep;
import lombok.Data;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@Data
@Slf4j
@SuperBuilder
public class DefaultSQLStrategy extends BaseSQLStrategy {

    private final String catalogName;

    private final String objectSchemaName;

    private final String historySchemaName;

    @Override
    public List<DDLStep> createEntityDDL(final DSLContext context, final Collection<? extends Schema<?>> schemas) {

        final List<DDLStep> queries = new ArrayList<>();

        queries.add(DDLStep.from(context.createSchemaIfNotExists(DSL.name(getObjectSchemaName()))));
        if (getHistorySchemaName() != null) {
            queries.add(DDLStep.from(context.createSchemaIfNotExists(DSL.name(getHistorySchemaName()))));
        }

        for (final Schema<?> schema : schemas) {

            if (schema instanceof LinkableSchema) {
                queries.addAll(createEntityDDL(context, (LinkableSchema) schema));
            } else if (schema instanceof FunctionSchema) {
                queries.addAll(createFunctionDDL(context, (FunctionSchema) schema));
            }
        }

        return queries;
    }

    @Override
    protected boolean isHistoryTableEnabled() {
        return getHistorySchemaName() != null;
    }
}
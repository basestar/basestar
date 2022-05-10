package io.basestar.storage.sql.strategy;

import com.google.common.collect.ImmutableSet;
import io.basestar.schema.*;
import io.basestar.schema.from.FromExternal;
import io.basestar.schema.from.FromSql;
import io.basestar.storage.sql.mapping.QueryMapping;
import io.basestar.storage.sql.util.DDLStep;
import io.basestar.util.Immutable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


@Data
@Slf4j
@SuperBuilder
@RequiredArgsConstructor
@AllArgsConstructor
public abstract class BaseSQLStrategy implements SQLStrategy {

    private final io.basestar.storage.sql.SQLDialect dialect;

    private final StatementType statementType;

    private final boolean useMetadata;

    private final boolean ignoreInvalidDDL;

    private EntityTypeStrategy entityTypeStrategy;

    private NamingStrategy namingStrategy;

    @Override
    public io.basestar.storage.sql.SQLDialect dialect() {

        return dialect;
    }

    @Override
    public StatementType statementType() {

        return statementType;
    }

    @Override
    public boolean useMetadata() {

        return useMetadata;
    }

    @Override
    public boolean ignoreInvalidDDL() {

        return useMetadata;
    }

    public EntityType entityType(final io.basestar.storage.sql.SQLDialect dialect, final LinkableSchema schema) {

        if (entityTypeStrategy == null) {
            return EntityTypeStrategy.DEFAULT.entityType(dialect, schema);
        } else {
            return entityTypeStrategy.entityType(dialect, schema);
        }
    }

    @Override
    public NamingStrategy getNamingStrategy() {

        return namingStrategy;
    }


    protected List<DDLStep> createEntityDDL(final DSLContext context, final LinkableSchema schema) {

        switch (entityType(dialect(), schema)) {
            case VIEW:
                return createViewDDL(context, (ViewSchema) schema);
            case MATERIALIZED_VIEW:
                return createMaterializedViewDDL(context, (ViewSchema) schema);
            case TABLE:
            default:
                if (schema instanceof ReferableSchema) {
                    return createObjectDDL(context, (ReferableSchema) schema);
                } else {
                    return createFakeViewDDL(context, schema);
                }
        }
    }

    protected List<DDLStep> createFunctionDDL(final DSLContext context, final FunctionSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();

        if (dialect.supportsUDFs()) {

            final Name functionName = namingStrategy.functionName(schema);

            log.info("Creating function {}", functionName);

            final String definition = schema.getReplacedDefinition((s, v) -> namingStrategy.reference(s, v).toString());
            final String sql = dialect().createFunctionDDL(context, functionName, schema.getReturns(), schema.getArguments(), schema.getLanguage(), definition);
            queries.add(DDLStep.from(context, sql));
        }
        return queries;
    }

    protected List<DDLStep> createSequenceDDL(final DSLContext context, final SequenceSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();

        if (dialect.supportsSequences()) {

            final Name sequenceName = namingStrategy.sequenceName(schema);

            log.info("Creating sequence {}", sequenceName);

            final String sql = dialect().createSequenceDDL(context, sequenceName, schema.getEffectiveStart(), schema.getEffectiveIncrement());
            queries.add(DDLStep.from(context, sql));
        }
        return queries;
    }

    protected List<DDLStep> createMaterializedViewDDL(final DSLContext context, final ViewSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();
        final Name viewName = namingStrategy.viewName(schema);

        log.info("Creating materialized view {}", viewName);

        final String query;
        if (schema.getFrom() instanceof FromSql) {
            query = ((FromSql) schema.getFrom()).getReplacedSql((s, v) -> namingStrategy.reference(s, v).toString());
        } else {
            throw new UnsupportedOperationException("Simple views no longer supported in SQL storage");
        }
        queries.add(DDLStep.from(context, "create or replace materialized view " + viewName + " as " + query));

        return queries;
    }

    protected List<DDLStep> createFakeViewDDL(final DSLContext context, final LinkableSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();
        final Name viewName = namingStrategy.entityName(schema);

        log.info("Creating fake view {}", viewName);

        final QueryMapping mapping = dialect.schemaMapping(schema, null, Immutable.set());
        final List<Field<?>> columns = mapping.defineFields(namingStrategy, ImmutableSet.of());

        queries.add(DDLStep.from(withPrimaryKey(schema, context.createTableIfNotExists(viewName)
                .columns(columns))));
        return queries;
    }


    protected List<DDLStep> createViewDDL(final DSLContext context, final ViewSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();
        final Name viewName = namingStrategy.viewName(schema);

        log.info("Creating view {}", viewName);

        if (schema.getFrom() instanceof FromSql) {
            queries.add(DDLStep.from(context.createOrReplaceView(viewName)
                    .as(DSL.sql(((FromSql) schema.getFrom()).getReplacedSql((s, v) -> namingStrategy.reference(s, v).toString())))));
        } else if (!(schema.getFrom() instanceof FromExternal)) {
            log.error("Simple views no longer supported in SQL storage");
        }

        return queries;
    }

    protected List<DDLStep> createObjectDDL(final DSLContext context, final ReferableSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();

        final Name objectTableName = getNamingStrategy().objectTableName(schema);
        final Optional<Name> historyTableName = getNamingStrategy().historyTableName(schema);

        final QueryMapping queryMapping = dialect.schemaMapping(schema, false, ImmutableSet.of());

        final List<Field<?>> columns = queryMapping.defineFields(namingStrategy, ImmutableSet.of());

        log.info("Creating table {}", objectTableName);
        queries.add(DDLStep.from(withPrimaryKey(schema, context.createTableIfNotExists(objectTableName)
                .columns(columns))));

//        if (dialect.supportsIndexes()) {
//            for (final Index index : schema.getIndexes().values()) {
//                if (index.isMultiValue()) {
//                    final Optional<Name> indexTableName = getNamingStrategy().indexTableName(schema, index);
//                    log.info("Creating multi-value index table {}", indexTableName);
//                    indexTableName.ifPresent(name -> queries.add(DDLStep.from(context.createTableIfNotExists(name)
//                            .columns(dialect.fields(columnCasing, schema, index))
//                            .constraints(dialect.primaryKey(columnCasing, schema, index)))));
//                } else if (index.isUnique()) {
//                    log.info("Creating unique index {}:{}", objectTableName, index.getName());
//                    queries.add(DDLStep.from(context.createUniqueIndexIfNotExists(index.getName())
//                            .on(DSL.table(objectTableName), dialect.indexKeys(columnCasing, schema, index))));
//                } else {
//                    log.info("Creating index {}:{}", objectTableName, index.getName());
//                    // Fixme
//                    if (!index.getName().equals("expanded")) {
//                        queries.add(DDLStep.from(context.createIndexIfNotExists(index.getName())
//                                .on(DSL.table(objectTableName), dialect.indexKeys(columnCasing, schema, index))));
//                    }
//                }
//            }
//        }

        if (isHistoryTableEnabled()) {
            historyTableName.ifPresent(name -> {

                log.info("Creating history table {}", name);

                queries.add(DDLStep.from(withHistoryPrimaryKey(schema, context.createTableIfNotExists(name)
                        .columns(columns))));
            });

        }

        return queries;
    }

    protected abstract boolean isHistoryTableEnabled();

    private CreateTableFinalStep withPrimaryKey(final LinkableSchema schema, final CreateTableColumnStep create) {

        if (dialect.supportsConstraints()) {
            return create.constraint(DSL.primaryKey(namingStrategy.columnName(schema.id())));
        } else {
            return create;
        }
    }

    private CreateTableFinalStep withHistoryPrimaryKey(final ReferableSchema schema, final CreateTableColumnStep create) {

        if (dialect.supportsConstraints()) {
            return create.constraint(DSL.primaryKey(namingStrategy.columnName(ObjectSchema.ID), namingStrategy.columnName(ObjectSchema.VERSION)));
        } else {
            return create;
        }
    }
}
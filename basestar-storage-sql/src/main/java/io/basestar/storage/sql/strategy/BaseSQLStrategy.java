package io.basestar.storage.sql.strategy;

import io.basestar.expression.constant.Constant;
import io.basestar.schema.Index;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.from.From;
import io.basestar.schema.from.FromSchema;
import io.basestar.schema.from.FromSql;
import io.basestar.schema.util.Casing;
import io.basestar.storage.sql.SQLExpressionVisitor;
import io.basestar.storage.sql.util.DDLStep;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@Data
@Slf4j
@SuperBuilder
@RequiredArgsConstructor
@AllArgsConstructor
public abstract class BaseSQLStrategy implements SQLStrategy {

    private final io.basestar.storage.sql.SQLDialect dialect;

    private final StatementType statementType;

    private final boolean useMetadata;

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
            final String definition = schema.getReplacedDefinition(s -> namingStrategy.reference(s).toString());
            final String sql = dialect().createFunctionDDL(context, namingStrategy.functionName(schema), schema.getReturns(), schema.getArguments(), schema.getLanguage(), definition);
            queries.add(DDLStep.from(context, sql));
        }
        return queries;
    }

    protected List<DDLStep> createMaterializedViewDDL(final DSLContext context, final ViewSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();
        final Name viewName = namingStrategy.viewName(schema);

        final String query;
        if (schema.getFrom() instanceof FromSql) {
            query = ((FromSql) schema.getFrom()).getReplacedSql(s -> namingStrategy.reference(s).toString());
        } else {
            query = viewQuery(context, schema).getSQL();
        }
        queries.add(DDLStep.from(context, "create or replace materialized view " + viewName + " as " + query));

        return queries;
    }

    protected List<DDLStep> createFakeViewDDL(final DSLContext context, final LinkableSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();
        final Name viewName = namingStrategy.entityName(schema);

        final Casing columnCasing = namingStrategy.getColumnCasing();

        final List<Field<?>> columns = dialect.fields(columnCasing, schema);

        queries.add(DDLStep.from(withPrimaryKey(schema, context.createTableIfNotExists(viewName)
                .columns(columns))));
        return queries;
    }


    protected List<DDLStep> createViewDDL(final DSLContext context, final ViewSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();
        final Name viewName = namingStrategy.viewName(schema);

        if (schema.getFrom() instanceof FromSql) {
            queries.add(DDLStep.from(context.createOrReplaceView(viewName)
                    .as(DSL.sql(((FromSql) schema.getFrom()).getReplacedSql(s -> namingStrategy.reference(s).toString())))));
        } else {
            queries.add(DDLStep.from(context.createOrReplaceView(viewName)
                    .as(viewQuery(context, schema))));
        }

        return queries;
    }

    protected Select<?> viewQuery(final DSLContext context, final ViewSchema schema) {

        final Collection<SelectFieldOrAsterisk> fields = new ArrayList<>();
        final InferenceContext inferenceContext = viewInferenceContext(schema);
        final SQLExpressionVisitor visitor = new SQLExpressionVisitor(dialect(), inferenceContext, name -> DSL.field(DSL.name(namingStrategy.columnName(name.first()))));
        final Field<?> idField = visitor.field(new Constant(null));
        fields.add(idField.cast(String.class).as(ViewSchema.ID));
        schema.getProperties().forEach((k, v) -> {
            final Field<?> field = visitor.field(v.getExpression());
            if (field == null) {
                log.error("Cannot create view field " + k + " " + v.getExpression());
            } else {
                fields.add(visitor.field(v.getExpression()).as(k));
            }
        });

        final SelectJoinStep<?> step = context.select(fields).from(viewFrom(schema));

        if (schema.isGrouping()) {
            final Collection<GroupField> groupFields = new ArrayList<>();
            schema.getGroup().forEach(k -> groupFields.add(DSL.field(DSL.name(namingStrategy.columnName(k)))));
            return step.groupBy(groupFields);
        } else {
            return step;
        }
    }

    private TableLike<?> viewFrom(final ViewSchema schema) {

        final From from = schema.getFrom();
        if (from instanceof FromSchema) {
            return DSL.table(namingStrategy.entityName(((FromSchema) from).getSchema()));
        } else {
            throw new UnsupportedOperationException("Cannot create view from " + schema.getFrom().getClass());
        }
    }

    private InferenceContext viewInferenceContext(final ViewSchema schema) {

        final From from = schema.getFrom();
        if (from instanceof FromSchema) {
            return InferenceContext.from(((FromSchema) from).getLinkableSchema());
        } else {
            throw new UnsupportedOperationException("Cannot create view from " + schema.getFrom().getClass());
        }
    }

    protected List<DDLStep> createObjectDDL(final DSLContext context, final ReferableSchema schema) {

        final List<DDLStep> queries = new ArrayList<>();

        final Casing columnCasing = namingStrategy.getColumnCasing();

        final Name objectTableName = getNamingStrategy().objectTableName(schema);
        final Name historyTableName = getNamingStrategy().historyTableName(schema);

        final List<Field<?>> columns = dialect.fields(columnCasing, schema); //rowMapper(schema, schema.getExpand()).columns();

        log.info("Creating table {}", objectTableName);
        queries.add(DDLStep.from(withPrimaryKey(schema, context.createTableIfNotExists(objectTableName)
                .columns(columns))));

        if (dialect.supportsIndexes()) {
            for (final Index index : schema.getIndexes().values()) {
                if (index.isMultiValue()) {
                    final Name indexTableName = getNamingStrategy().indexTableName(schema, index);
                    log.info("Creating multi-value index table {}", indexTableName);
                    queries.add(DDLStep.from(context.createTableIfNotExists(indexTableName)
                            .columns(dialect.fields(columnCasing, schema, index))
                            .constraints(dialect.primaryKey(columnCasing, schema, index))));
                } else if (index.isUnique()) {
                    log.info("Creating unique index {}:{}", objectTableName, index.getName());
                    queries.add(DDLStep.from(context.createUniqueIndexIfNotExists(index.getName())
                            .on(DSL.table(objectTableName), dialect.indexKeys(columnCasing, schema, index))));
                } else {
                    log.info("Creating index {}:{}", objectTableName, index.getName());
                    // Fixme
                    if (!index.getName().equals("expanded")) {
                        queries.add(DDLStep.from(context.createIndexIfNotExists(index.getName())
                                .on(DSL.table(objectTableName), dialect.indexKeys(columnCasing, schema, index))));
                    }
                }
            }
        }

        if (isHistoryTableEnabled()) {
            log.info("Creating history table {}", historyTableName);
            queries.add(DDLStep.from(withHistoryPrimaryKey(schema, context.createTableIfNotExists(historyTableName)
                    .columns(columns))));
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
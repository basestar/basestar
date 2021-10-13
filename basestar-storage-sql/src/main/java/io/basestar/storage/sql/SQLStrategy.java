package io.basestar.storage.sql;

/*-
 * #%L
 * basestar-storage-sql
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.basestar.expression.constant.Constant;
import io.basestar.schema.Index;
import io.basestar.schema.Schema;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.from.From;
import io.basestar.schema.from.FromSchema;
import io.basestar.schema.from.FromSql;
import io.basestar.storage.sql.util.DDLStep;
import io.basestar.util.Nullsafe;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public interface SQLStrategy {

    org.jooq.Name catalogName(ReferableSchema schema);

    org.jooq.Name objectTableName(ReferableSchema schema);

    org.jooq.Name historyTableName(ReferableSchema schema);

    org.jooq.Name functionName(FunctionSchema schema);

    org.jooq.Name viewName(ViewSchema schema);

    default org.jooq.Name entityName(final Schema<?> schema) {

        if (schema instanceof ReferableSchema) {
            return objectTableName((ReferableSchema) schema);
        } else if (schema instanceof ViewSchema) {
            return viewName((ViewSchema) schema);
        } else if (schema instanceof FunctionSchema) {
            return functionName((FunctionSchema) schema);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    // Only used for multi-value indexes
    org.jooq.Name indexTableName(ReferableSchema schema, Index index);

    default void createEntities(final DSLContext context, final Collection<? extends Schema<?>> schemas) {

        final Logger log = LoggerFactory.getLogger(SQLStrategy.class);
        for (final DDLStep query : createEntityDDL(context, schemas)) {
            try {
                query.execute();
            } catch (final Exception e) {
                log.error("Failed to execute DDL query " + query.getSQL(), e);
            }
        }
    }

    List<DDLStep> createEntityDDL(DSLContext context, Collection<? extends Schema<?>> schemas);

    SQLDialect dialect();

    StatementType statementType();

    boolean useMetadata();

    @Data
    @Slf4j
    @Builder(builderClassName = "Builder")
    class Simple implements SQLStrategy {

        public static enum Casing {

            AS_SPECIFIED,
            LOWERCASE,
            UPPERCASE
        }

        private static final String CUSTOM_TABLE_NAME_EXTENSION = "sql.table";

        private final String catalogName;

        private final String objectSchemaName;

        @Nullable
        private final String historySchemaName;

        private final SQLDialect dialect;

        private final StatementType statementType;

        private final boolean useMetadata;

        private final Casing casing;

        private EntityTypeStrategy entityTypeStrategy;

        private String name(final Schema<?> schema) {

            final String name = schema.getQualifiedName().toString("_");
            switch (Nullsafe.orDefault(casing, Casing.AS_SPECIFIED)) {
                case LOWERCASE:
                    return name.toLowerCase();
                case UPPERCASE:
                    return name.toUpperCase();
                case AS_SPECIFIED:
                default:
                    return name;
            }
        }

        private org.jooq.Name customizeName(final Schema<?> schema, final Supplier<org.jooq.Name> defaultName) {

            return schema.getOptionalExtension(String.class, CUSTOM_TABLE_NAME_EXTENSION).map(SQLUtils::parseName).orElseGet(defaultName);
        }

        private org.jooq.Name combineNames(final org.jooq.Name schemaName, final org.jooq.Name name) {

            if (name.getName().length == 1) {
                return DSL.name(schemaName, name);
            } else {
                return name;
            }
        }

        @Override
        public Name catalogName(final ReferableSchema schema) {

            return DSL.name(catalogName);
        }

        @Override
        public org.jooq.Name objectTableName(final ReferableSchema schema) {

            return combineNames(DSL.name(objectSchemaName), customizeName(schema, () -> DSL.name(name(schema))));
        }

        @Override
        public org.jooq.Name functionName(final FunctionSchema schema) {

            return combineNames(DSL.name(objectSchemaName), customizeName(schema, () -> DSL.name(name(schema))));
        }

        @Override
        public Name viewName(final ViewSchema schema) {

            return combineNames(DSL.name(objectSchemaName), customizeName(schema, () -> DSL.name(name(schema))));
        }

        @Override
        public org.jooq.Name historyTableName(final ReferableSchema schema) {

            return combineNames(DSL.name(historySchemaName), DSL.name(name(schema)));
        }

        @Override
        public org.jooq.Name indexTableName(final ReferableSchema schema, final Index index) {

            final String name = name(schema) + Reserved.PREFIX + index.getName();
            return combineNames(DSL.name(objectSchemaName), DSL.name(name));
        }

        @Override
        public SQLDialect dialect() {

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

        public EntityType entityType(final SQLDialect dialect, final LinkableSchema schema) {

            if(entityTypeStrategy == null) {
                return EntityTypeStrategy.DEFAULT.entityType(dialect, schema);
            } else {
                return entityTypeStrategy.entityType(dialect, schema);
            }
        }

        @Override
        public List<DDLStep> createEntityDDL(final DSLContext context, final Collection<? extends Schema<?>> schemas) {

            final List<DDLStep> queries = new ArrayList<>();

            queries.add(DDLStep.from(context.createSchemaIfNotExists(DSL.name(objectSchemaName))));
            if (historySchemaName != null) {
                queries.add(DDLStep.from(context.createSchemaIfNotExists(DSL.name(historySchemaName))));
            }

            for (final Schema<?> schema : schemas) {

                if(schema instanceof LinkableSchema) {
                    queries.addAll(createEntityDDL(context, (LinkableSchema) schema));
                } else if (schema instanceof FunctionSchema) {
                    queries.addAll(createFunctionDDL(context, (FunctionSchema) schema));
                }
            }

            return queries;
        }

        protected List<DDLStep> createEntityDDL(final DSLContext context, final LinkableSchema schema) {

            switch (entityType(dialect(), schema)) {
                case VIEW:
                    return createViewDDL(context, (ViewSchema)schema);
                case MATERIALIZED_VIEW:
                    return createMaterializedViewDDL(context, (ViewSchema)schema);
                case TABLE:
                default:
                    if(schema instanceof ReferableSchema) {
                        return createObjectDDL(context, (ReferableSchema) schema);
                    } else {
                        return createFakeViewDDL(context, schema);
                    }
            }
        }

        protected List<DDLStep> createFunctionDDL(final DSLContext context, final FunctionSchema schema) {

            final List<DDLStep> queries = new ArrayList<>();
            if (dialect.supportsUDFs()) {
                final String definition = schema.getReplacedDefinition(s -> entityName(s).toString());
                final String sql = dialect().createFunctionDDL(context, functionName(schema), schema.getReturns(), schema.getArguments(), schema.getLanguage(), definition);
                queries.add(DDLStep.from(context, sql));
            }
            return queries;
        }

        protected List<DDLStep> createMaterializedViewDDL(final DSLContext context, final ViewSchema schema) {

            final List<DDLStep> queries = new ArrayList<>();
            final org.jooq.Name viewName = viewName(schema);

            final String query;
            if (schema.getFrom() instanceof FromSql) {
                query = ((FromSql) schema.getFrom()).getReplacedSql(s -> entityName(s).toString());
            } else {
                query = viewQuery(context, schema).getSQL();
            }
            queries.add(DDLStep.from(context, "create or replace materialized view " + viewName + " as " + query));

            return queries;
        }

        protected List<DDLStep> createFakeViewDDL(final DSLContext context, final LinkableSchema schema) {

            final List<DDLStep> queries = new ArrayList<>();
            final org.jooq.Name viewName = entityName(schema);

            final List<Field<?>> columns = dialect.fields(schema);

            queries.add(DDLStep.from(withPrimaryKey(schema, context.createTableIfNotExists(viewName)
                    .columns(columns))));
            return queries;
        }

        protected List<DDLStep> createViewDDL(final DSLContext context, final ViewSchema schema) {

            final List<DDLStep> queries = new ArrayList<>();
            final org.jooq.Name viewName = viewName(schema);

            if (schema.getFrom() instanceof FromSql) {
                queries.add(DDLStep.from(context.createOrReplaceView(viewName)
                        .as(DSL.sql(((FromSql) schema.getFrom()).getReplacedSql(s -> entityName(s).toString())))));
            } else {
                queries.add(DDLStep.from(context.createOrReplaceView(viewName)
                        .as(viewQuery(context, schema))));
            }

            return queries;
        }

        protected Select<?> viewQuery(final DSLContext context, final ViewSchema schema) {

            final Collection<SelectFieldOrAsterisk> fields = new ArrayList<>();
            final InferenceContext inferenceContext = viewInferenceContext(schema);
            final SQLExpressionVisitor visitor = new SQLExpressionVisitor(dialect(), inferenceContext, name -> DSL.field(DSL.name(name.first())));
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
                schema.getGroup().forEach(k -> groupFields.add(DSL.field(DSL.name(k))));
                return step.groupBy(groupFields);
            } else {
                return step;
            }
        }

        private TableLike<?> viewFrom(final ViewSchema schema) {

            final From from = schema.getFrom();
            if (from instanceof FromSchema) {
                return DSL.table(entityName(((FromSchema) from).getSchema()));
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

            final org.jooq.Name objectTableName = objectTableName(schema);
            final org.jooq.Name historyTableName = historyTableName(schema);

            final List<Field<?>> columns = dialect.fields(schema); //rowMapper(schema, schema.getExpand()).columns();

            log.info("Creating table {}", objectTableName);
            queries.add(DDLStep.from(withPrimaryKey(schema, context.createTableIfNotExists(objectTableName)
                    .columns(columns))));

            if (dialect.supportsIndexes()) {
                for (final Index index : schema.getIndexes().values()) {
                    if (index.isMultiValue()) {
                        final org.jooq.Name indexTableName = indexTableName(schema, index);
                        log.info("Creating multi-value index table {}", indexTableName);
                        queries.add(DDLStep.from(context.createTableIfNotExists(indexTableName(schema, index))
                                .columns(dialect.fields(schema, index))
                                .constraints(dialect.primaryKey(schema, index))));
                    } else if (index.isUnique()) {
                        log.info("Creating unique index {}:{}", objectTableName, index.getName());
                        queries.add(DDLStep.from(context.createUniqueIndexIfNotExists(index.getName())
                                .on(DSL.table(objectTableName), dialect.indexKeys(schema, index))));
                    } else {
                        log.info("Creating index {}:{}", objectTableName, index.getName());
                        // Fixme
                        if (!index.getName().equals("expanded")) {
                            queries.add(DDLStep.from(context.createIndexIfNotExists(index.getName())
                                    .on(DSL.table(objectTableName), dialect.indexKeys(schema, index))));
                        }
                    }
                }
            }

            if (historySchemaName != null) {
                log.info("Creating history table {}", historyTableName);
                queries.add(DDLStep.from(withHistoryPrimaryKey(schema, context.createTableIfNotExists(historyTableName)
                        .columns(columns))));
            }

            return queries;
        }

        private CreateTableFinalStep withPrimaryKey(final LinkableSchema schema, final CreateTableColumnStep create) {

            if (dialect.supportsConstraints()) {
                return create.constraint(DSL.primaryKey(schema.id()));
            } else {
                return create;
            }
        }

        private CreateTableFinalStep withHistoryPrimaryKey(final ReferableSchema schema, final CreateTableColumnStep create) {

            if (dialect.supportsConstraints()) {
                return create.constraint(DSL.primaryKey(ObjectSchema.ID, ObjectSchema.VERSION));
            } else {
                return create;
            }
        }
    }

    enum EntityType {

        TABLE,
        VIEW,
        MATERIALIZED_VIEW
    }

    interface EntityTypeStrategy {

        Default DEFAULT = new Default();

        EntityType entityType(SQLDialect dialect, LinkableSchema schema);

        class Default implements EntityTypeStrategy {

            @Override
            public EntityType entityType(final SQLDialect dialect, final LinkableSchema schema) {

                if(schema instanceof ViewSchema) {
                    final ViewSchema view = (ViewSchema) schema;
                    if(view.isMaterialized()) {
                        return dialect.supportsMaterializedView(view) ? EntityType.MATERIALIZED_VIEW : EntityType.TABLE;
                    } else {
                        return EntityType.VIEW;
                    }
                } else {
                    return EntityType.TABLE;
                }
            }
        }
    }
}

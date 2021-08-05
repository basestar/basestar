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
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface SQLStrategy {

    org.jooq.Name objectTableName(ReferableSchema schema);

    org.jooq.Name historyTableName(ReferableSchema schema);

    org.jooq.Name viewName(ViewSchema schema);

    default org.jooq.Name tableName(final LinkableSchema schema) {

        if (schema instanceof ReferableSchema) {
            return objectTableName((ReferableSchema) schema);
        } else if (schema instanceof ViewSchema) {
            return viewName((ViewSchema) schema);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    // Only used for multi-value indexes
    org.jooq.Name indexTableName(ReferableSchema schema, Index index);

    void createTables(DSLContext context, Collection<? extends Schema<?>> schemas);

    SQLDialect dialect();

    @Data
    @Slf4j
    @Builder(builderClassName = "Builder")
    class Simple implements SQLStrategy {

        private final String objectSchemaName;

        private final String historySchemaName;

        private final SQLDialect dialect;

        private String name(final LinkableSchema schema) {

            return schema.getQualifiedName().toString("_").toLowerCase();
        }

        @Override
        public org.jooq.Name objectTableName(final ReferableSchema schema) {

            return DSL.name(DSL.name(objectSchemaName), DSL.name(name(schema)));
        }

        @Override
        public Name viewName(final ViewSchema schema) {

            return DSL.name(DSL.name(objectSchemaName), DSL.name(name(schema)));
        }

        @Override
        public org.jooq.Name historyTableName(final ReferableSchema schema) {

            return DSL.name(DSL.name(historySchemaName), DSL.name(name(schema)));
        }

        @Override
        public org.jooq.Name indexTableName(final ReferableSchema schema, final Index index) {

            final String name = name(schema) + Reserved.PREFIX + index.getName();
            return DSL.name(DSL.name(objectSchemaName), DSL.name(name));
        }

        @Override
        public SQLDialect dialect() {

            return dialect;
        }

        @Override
        public void createTables(final DSLContext context, final Collection<? extends Schema<?>> schemas) {

            try (final CreateSchemaFinalStep create = context.createSchemaIfNotExists(DSL.name(objectSchemaName))) {
                create.execute();
            } catch (final Exception e) {
                log.error("Failed to create object schema", e);
            }

            try (final CreateSchemaFinalStep create = context.createSchemaIfNotExists(DSL.name(historySchemaName))) {
                create.execute();
            } catch (final Exception e) {
                log.error("Failed to create history schema", e);
            }

            for (final Schema<?> schema : schemas) {

                if (schema instanceof ReferableSchema) {

                    createObjectTable(context, (ReferableSchema) schema);

                } else if (schema instanceof ViewSchema) {

                    createViewTable(context, (ViewSchema) schema);
                }
            }
        }

        private void createViewTable(final DSLContext context, final ViewSchema schema) {

            final org.jooq.Name viewName = viewName(schema);
            if (schema.isMaterialized()) {

                final List<Field<?>> columns = dialect.fields(schema);

                try (final CreateTableFinalStep create = withPrimaryKey(schema, context.createTableIfNotExists(viewName)
                        .columns(columns))) {
                    create.execute();
                } catch (final Exception e) {
                    log.error("Failed to create view table {}", schema.getQualifiedName(), e);
                }

            } else {

                if (schema.getFrom() instanceof FromSql) {
                    log.info("Inline SQL views not currently supported (must be manually created in storage)");
                } else {

                    try (final CreateViewFinalStep create = context.createOrReplaceView(viewName).as(viewQuery(context, schema))) {
                        create.execute();
                    } catch (final Exception e) {
                        log.error("Failed to create view {}", schema.getQualifiedName(), e);
                    }
                }
            }
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
                return DSL.table(tableName(((FromSchema) from).getSchema()));
            } else {
                throw new UnsupportedOperationException("Cannot create view from " + schema.getFrom().getClass());
            }
        }

        private InferenceContext viewInferenceContext(final ViewSchema schema) {

            final From from = schema.getFrom();
            if (from instanceof FromSchema) {
                return InferenceContext.from(((FromSchema) from).getSchema());
            } else {
                throw new UnsupportedOperationException("Cannot create view from " + schema.getFrom().getClass());
            }
        }

        private void createObjectTable(final DSLContext context, final ReferableSchema schema) {

            final org.jooq.Name objectTableName = objectTableName(schema);
            final org.jooq.Name historyTableName = historyTableName(schema);

            final List<Field<?>> columns = dialect.fields(schema); //rowMapper(schema, schema.getExpand()).columns();

            log.info("Creating table {}", objectTableName);
            try (final CreateTableFinalStep create = withPrimaryKey(schema, context.createTableIfNotExists(objectTableName)
                    .columns(columns))) {
                create.execute();
            } catch (final Exception e) {
                log.error("Failed to create object table {}", schema.getQualifiedName(), e);
            }

            if (dialect.supportsIndexes()) {
                for (final Index index : schema.getIndexes().values()) {
                    if (index.isMultiValue()) {
                        final org.jooq.Name indexTableName = indexTableName(schema, index);
                        log.info("Creating multi-value index table {}", indexTableName);
                        try (final CreateTableFinalStep create = context.createTableIfNotExists(indexTableName(schema, index))
                                .columns(dialect.fields(schema, index))
                                .constraints(dialect.primaryKey(schema, index))) {
                            create.execute();
                        } catch (final Exception e) {
                            log.error("Failed to create index table {}.{}", schema.getQualifiedName(), index.getName(), e);
                        }
                    } else if (index.isUnique()) {
                        log.info("Creating unique index {}:{}", objectTableName, index.getName());
                        try (final CreateIndexFinalStep create = context.createUniqueIndexIfNotExists(index.getName())
                                .on(DSL.table(objectTableName), dialect.indexKeys(schema, index))) {
                            create.execute();
                        } catch (final Exception e) {
                            log.error("Failed to create unique index {}.{}", schema.getQualifiedName(), index.getName(), e);
                        }
                    } else {
                        log.info("Creating index {}:{}", objectTableName, index.getName());
                        // Fixme
                        if (!index.getName().equals("expanded")) {
                            try (final CreateIndexFinalStep create = context.createIndexIfNotExists(index.getName())
                                    .on(DSL.table(objectTableName), dialect.indexKeys(schema, index))) {
                                create.execute();
                            } catch (final Exception e) {
                                log.error("Failed to create index {}.{}", schema.getQualifiedName(), index.getName(), e);
                            }
                        }
                    }
                }
            }

            log.info("Creating table {}", historyTableName);
            try (final CreateTableFinalStep create = withHistoryPrimaryKey(schema, context.createTableIfNotExists(historyTableName)
                    .columns(columns))) {
                create.execute();
            }
        }

        private CreateTableFinalStep withPrimaryKey(final LinkableSchema schema, final CreateTableColumnStep create) {

            if(dialect.supportsConstraints()) {
                return create.constraint(DSL.primaryKey(schema.id()));
            } else {
                return create;
            }
        }

        private CreateTableFinalStep withHistoryPrimaryKey(final ReferableSchema schema, final CreateTableColumnStep create) {

            if(dialect.supportsConstraints()) {
                return create.constraint(DSL.primaryKey(ObjectSchema.ID, ObjectSchema.VERSION));
            } else {
                return create;
            }
        }
    }
}

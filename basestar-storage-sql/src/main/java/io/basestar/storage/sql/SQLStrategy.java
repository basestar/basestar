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

import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Reserved;
import io.basestar.storage.sql.mapper.ColumnStrategy;
import io.basestar.storage.sql.mapper.RowMapper;
import io.basestar.util.Name;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SQLStrategy {

    org.jooq.Name objectTableName(ReferableSchema schema);

    org.jooq.Name historyTableName(ReferableSchema schema);

    // Only used for multi-value indexes
    org.jooq.Name indexTableName(ReferableSchema schema, Index index);

    void createTables(DSLContext context, Collection<ReferableSchema> schemas);

    RowMapper<Map<String, Object>> rowMapper(ReferableSchema schema, Set<Name> expand);

    @Data
    @Slf4j
    @Builder(builderClassName = "Builder")
    class Simple implements SQLStrategy {

        private final String objectSchemaName;

        private final String historySchemaName;

        private final ColumnStrategy columnStrategy;

        private String name(final ReferableSchema schema) {

            return schema.getQualifiedName().toString(Reserved.PREFIX);
        }

        @Override
        public org.jooq.Name objectTableName(final ReferableSchema schema) {

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
        public RowMapper<Map<String, Object>> rowMapper(final ReferableSchema schema, final Set<Name> expand) {

            return RowMapper.forInstance(columnStrategy, schema, expand);
        }

        @Override
        public void createTables(final DSLContext context, final Collection<ReferableSchema> schemas) {

            try(final CreateSchemaFinalStep create = context.createSchemaIfNotExists(DSL.name(objectSchemaName))) {
                create.execute();
            }
            try(final CreateSchemaFinalStep create = context.createSchemaIfNotExists(DSL.name(historySchemaName))) {
                create.execute();
            }

            for(final ReferableSchema schema : schemas) {

                final org.jooq.Name objectTableName = objectTableName(schema);
                final org.jooq.Name historyTableName = historyTableName(schema);

                final List<Field<?>> columns = SQLUtils.fields(schema); //rowMapper(schema, schema.getExpand()).columns();

                log.info("Creating table {}", objectTableName);
                try(final CreateTableFinalStep create = context.createTableIfNotExists(objectTableName)
                        .columns(columns)
                        .constraint(DSL.primaryKey(ObjectSchema.ID))) {
                    create.execute();
                }

                for(final Index index : schema.getIndexes().values()) {
                    if(index.isMultiValue()) {
                        final org.jooq.Name indexTableName = indexTableName(schema, index);
                        log.info("Creating multi-value index table {}", indexTableName);
                        try(final CreateTableFinalStep create = context.createTableIfNotExists(indexTableName(schema, index))
                                .columns(SQLUtils.fields(schema, index))
                                .constraints(SQLUtils.primaryKey(schema, index))) {
                            create.execute();
                        }
                    } else if(index.isUnique()) {
                        log.info("Creating unique index {}:{}", objectTableName, index.getName());
                        try(final CreateIndexFinalStep create = context.createUniqueIndexIfNotExists(index.getName())
                                .on(DSL.table(objectTableName), SQLUtils.indexKeys(schema, index))) {
                            create.execute();
                        }
                    } else {
                        log.info("Creating index {}:{}", objectTableName, index.getName());
                        try(final CreateIndexFinalStep create = context.createIndexIfNotExists(index.getName())
                                .on(DSL.table(objectTableName), SQLUtils.indexKeys(schema, index))) {
                            create.execute();
                        } catch (final DataAccessException e) {
                            // FIXME
                            log.error("Failed to create index", e);
                        }
                    }
                }

                log.info("Creating table {}", historyTableName);
                try(final CreateTableFinalStep create = context.createTableIfNotExists(historyTableName)
                        .columns(columns)
                        .constraint(DSL.primaryKey(ObjectSchema.ID, ObjectSchema.VERSION))) {
                    create.execute();
                }
            }
        }
    }
}

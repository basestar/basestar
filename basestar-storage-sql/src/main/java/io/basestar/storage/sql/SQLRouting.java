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
import io.basestar.schema.Reserved;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Name;
import org.jooq.impl.DSL;

import java.util.Collection;

public interface SQLRouting {

    Name objectTableName(ObjectSchema schema);

    Name historyTableName(ObjectSchema schema);

    // Only used for multi-value indexes
    Name indexTableName(ObjectSchema schema, Index index);

    void createTables(DSLContext context, Collection<ObjectSchema> schemas);

    @Data
    @Slf4j
    class Simple implements SQLRouting {

        private final String objectSchemaName;

        private final String historySchemaName;

        @Override
        public Name objectTableName(final ObjectSchema schema) {

            return DSL.name(DSL.name(objectSchemaName), DSL.name(schema.getName()));
        }

        @Override
        public Name historyTableName(final ObjectSchema schema) {

            return DSL.name(DSL.name(historySchemaName), DSL.name(schema.getName()));
        }

        @Override
        public Name indexTableName(final ObjectSchema schema, final Index index) {

            final String name = schema.getName() + Reserved.PREFIX + index.getName();
            return DSL.name(DSL.name(objectSchemaName), DSL.name(name));
        }

        @Override
        public void createTables(final DSLContext context, final Collection<ObjectSchema> schemas) {

            context.createSchemaIfNotExists(DSL.name(objectSchemaName)).execute();
            context.createSchemaIfNotExists(DSL.name(historySchemaName)).execute();

            for(final ObjectSchema schema : schemas) {

                final Name objectTableName = DSL.name(DSL.name(objectSchemaName), DSL.name(schema.getName()));
                final Name historyTableName = DSL.name(DSL.name(historySchemaName), DSL.name(schema.getName()));

                log.info("Creating table {}", objectTableName);
                context.createTableIfNotExists(objectTableName)
                        .columns(SQLUtils.fields(schema))
                        .constraint(DSL.primaryKey(Reserved.ID))
                        .execute();

                for(final Index index : schema.getAllIndexes().values()) {
                    if(index.isMultiValue()) {
                        final Name indexTableName = indexTableName(schema, index);
                        log.info("Creating multi-value index table {}", indexTableName);
                        context.createTableIfNotExists(indexTableName(schema, index))
                                .columns(SQLUtils.fields(schema, index))
                                .constraints(SQLUtils.primaryKey(schema, index))
                                .execute();
                    } else if(index.isUnique()) {
                        log.info("Creating unique index {}:{}", objectTableName, index.getName());
                        context.createUniqueIndexIfNotExists(index.getName())
                                .on(DSL.table(objectTableName), SQLUtils.orderFields(index))
                                .execute();
                    } else {
                        log.info("Creating index {}:{}", objectTableName, index.getName());
                        context.createIndexIfNotExists(index.getName())
                                .on(DSL.table(objectTableName), SQLUtils.orderFields(index))
                                .execute();
                    }

                }

                log.info("Creating table {}", historyTableName);
                context.createTableIfNotExists(historyTableName)
                        .columns(SQLUtils.fields(schema))
                        .constraint(DSL.primaryKey(Reserved.ID, Reserved.VERSION))
                        .execute();
            }
        }

    }
}

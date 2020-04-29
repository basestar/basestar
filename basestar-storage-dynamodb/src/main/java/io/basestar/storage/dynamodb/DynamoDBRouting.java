package io.basestar.storage.dynamodb;

/*-
 * #%L
 * basestar-storage-dynamodb
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

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import lombok.Data;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface DynamoDBRouting extends Serializable {

    String objectTableName(ObjectSchema schema);

    String historyTableName(ObjectSchema schema);

    String indexTableName(ObjectSchema schema, Index index);

    String objectPartitionPrefix(ObjectSchema schema);

    String historyPartitionPrefix(ObjectSchema schema);

    String indexPartitionPrefix(ObjectSchema schema, Index index);

    String objectPartitionName(ObjectSchema schema);

    String historyPartitionName(ObjectSchema schema);

    String historySortName(ObjectSchema schema);

    String indexPartitionName(ObjectSchema schema, Index index);

    String indexSortName(ObjectSchema schema, Index index);

    IndexType indexType(ObjectSchema schema, Index index);

    Map<String, TableDescription> tables(List<ObjectSchema> schemas);

    enum IndexType {

        EXT,
        GSI
    }

    @Data
    class SingleTable implements DynamoDBRouting {

        public static final String OBJECT_PARTITION_KEY = Reserved.PREFIX + "partition";

        public static final String HISTORY_PARTITION_KEY = OBJECT_PARTITION_KEY;

        public static final String HISTORY_SORT_KEY = Reserved.VERSION;

        public static final String INDEX_PARTITION_KEY = OBJECT_PARTITION_KEY;

        public static final String INDEX_SORT_KEY = Reserved.PREFIX + "sort";

        private final String tablePrefix;

        private final String dataPrefix;

        @lombok.Builder(builderClassName = "Builder")
        SingleTable(final String tablePrefix, final String dataPrefix) {

            this.tablePrefix = tablePrefix;
            this.dataPrefix = dataPrefix;
        }

        @Override
        public String objectPartitionPrefix(final ObjectSchema schema) {

            return dataPrefix + schema.getName() + Reserved.DELIMITER + schema.getVersion();
        }

        @Override
        public String historyPartitionPrefix(final ObjectSchema schema) {

            return objectPartitionPrefix(schema);
        }

        @Override
        public String indexPartitionPrefix(final ObjectSchema schema, final Index index) {

            return  dataPrefix + schema.getName() + Reserved.DELIMITER + schema.getVersion()
                    + Reserved.DELIMITER + index.getName() + Reserved.DELIMITER + index.getVersion();
        }

        @Override
        public String objectPartitionName(final ObjectSchema schema) {

            return OBJECT_PARTITION_KEY;
        }

        @Override
        public String historyPartitionName(final ObjectSchema schema) {

            return HISTORY_PARTITION_KEY;
        }

        @Override
        public String historySortName(final ObjectSchema schema) {

            return HISTORY_SORT_KEY;
        }

        @Override
        public String indexPartitionName(final ObjectSchema schema, final Index index) {

            return INDEX_PARTITION_KEY;
        }

        @Override
        public String indexSortName(final ObjectSchema schema, final Index index) {

            return INDEX_SORT_KEY;
        }

        @Override
        public IndexType indexType(final ObjectSchema schema, final Index index) {

            return IndexType.EXT;
        }

        @Override
        public String objectTableName(final ObjectSchema schema) {

            return tablePrefix + "Object";
        }

        @Override
        public String historyTableName(final ObjectSchema schema) {

            return tablePrefix + "History";
        }

        @Override
        public String indexTableName(final ObjectSchema schema, final Index index) {

            return tablePrefix + "Index";
        }

        @Override
        public Map<String, TableDescription> tables(final List<ObjectSchema> schemas) {

            return ImmutableMap.of(
                    "Object", TableDescription.builder()
                            .tableName(objectTableName(null))
                            .keySchema(DynamoDBUtils.keySchemaElement(OBJECT_PARTITION_KEY, KeyType.HASH))
                            .attributeDefinitions(DynamoDBUtils.attributeDefinition(OBJECT_PARTITION_KEY, ScalarAttributeType.S))
                            .build(),
                    "History", TableDescription.builder()
                            .tableName(historyTableName(null))
                            .keySchema(
                                    DynamoDBUtils.keySchemaElement(HISTORY_PARTITION_KEY, KeyType.HASH),
                                    DynamoDBUtils.keySchemaElement(HISTORY_SORT_KEY, KeyType.RANGE)
                            )
                            .attributeDefinitions(
                                    DynamoDBUtils.attributeDefinition(HISTORY_PARTITION_KEY, ScalarAttributeType.S),
                                    DynamoDBUtils.attributeDefinition(HISTORY_SORT_KEY, ScalarAttributeType.N)
                            )
                            .build(),
                    "Index", TableDescription.builder()
                            .tableName(indexTableName(null, null))
                            .keySchema(
                                    DynamoDBUtils.keySchemaElement(INDEX_PARTITION_KEY, KeyType.HASH),
                                    DynamoDBUtils.keySchemaElement(INDEX_SORT_KEY, KeyType.RANGE)
                            )
                            .attributeDefinitions(
                                    DynamoDBUtils.attributeDefinition(INDEX_PARTITION_KEY, ScalarAttributeType.B),
                                    DynamoDBUtils.attributeDefinition(INDEX_SORT_KEY, ScalarAttributeType.B)
                            )
                            .build()
            );
        }
    }

    @Data
    class MultiTable implements DynamoDBRouting {

        public static final String NAME_DELIMITER = ".";

        public static final String OBJECT_PARTITION_KEY = Reserved.ID;

        public static final String HISTORY_PARTITION_KEY = OBJECT_PARTITION_KEY;

        public static final String HISTORY_SORT_KEY = Reserved.VERSION;

        public static final String EXT_INDEX_PARTITION_KEY = Reserved.PREFIX + "partition";

        public static final String EXT_INDEX_SORT_KEY = Reserved.PREFIX + "sort";

        private final String tablePrefix;

        @lombok.Builder(builderClassName = "Builder")
        MultiTable(final String tablePrefix) {

            this.tablePrefix = tablePrefix;
        }

        @Override
        public String objectPartitionPrefix(final ObjectSchema schema) {

            return null;
        }

        @Override
        public String historyPartitionPrefix(final ObjectSchema schema) {

            return null;
        }

        @Override
        public String indexPartitionPrefix(final ObjectSchema schema, final Index index) {

            return null;
        }

        @Override
        public String objectPartitionName(final ObjectSchema schema) {

            return OBJECT_PARTITION_KEY;
        }

        @Override
        public String historyPartitionName(final ObjectSchema schema) {

            return HISTORY_PARTITION_KEY;
        }

        @Override
        public String historySortName(final ObjectSchema schema) {

            return HISTORY_PARTITION_KEY;
        }

        @Override
        public String indexPartitionName(final ObjectSchema schema, final Index index) {

            switch(indexType(schema, index)) {
                case EXT:
                    return EXT_INDEX_PARTITION_KEY;
                case GSI:
                    return gsiPartitionName(index);
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public String indexSortName(final ObjectSchema schema, final Index index) {

            switch(indexType(schema, index)) {
                case EXT:
                    return EXT_INDEX_SORT_KEY;
                case GSI:
                    return gsiSortName(index);
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private static String gsiPartitionName(final Index index) {

            return Reserved.PREFIX + "partition" + NAME_DELIMITER + index.getName() + NAME_DELIMITER + index.getVersion();
        }

        private static String gsiSortName(final Index index) {

            return Reserved.PREFIX + "sort" + NAME_DELIMITER + index.getName() + NAME_DELIMITER + index.getVersion();
        }

        @Override
        public String objectTableName(final ObjectSchema schema) {

            return tablePrefix + schema.getName() + NAME_DELIMITER + schema.getVersion();
        }

        @Override
        public String historyTableName(final ObjectSchema schema) {

            return tablePrefix + schema.getName() + NAME_DELIMITER + schema.getVersion()
                    + NAME_DELIMITER + "History";
        }

        @Override
        public String indexTableName(final ObjectSchema schema, final Index index) {

            switch(indexType(schema, index)) {
                case EXT:
                    return tablePrefix + schema.getName() + NAME_DELIMITER + schema.getVersion()
                            + NAME_DELIMITER + index.getName() + NAME_DELIMITER + index.getVersion();
                case GSI:
                    return index.getName() + NAME_DELIMITER + index.getVersion();
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public IndexType indexType(final ObjectSchema schema, final Index index) {

            if(index.getConsistency() == Consistency.EVENTUAL) {
                return IndexType.GSI;
            } else {
                return IndexType.EXT;
            }
        }

        @Override
        public Map<String, TableDescription> tables(final List<ObjectSchema> schemas) {

            final Map<String, TableDescription> tables = new HashMap<>();

            schemas.forEach(schema -> {

                final List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
                final List<GlobalSecondaryIndexDescription> gsis = new ArrayList<>();
                schema.getAllIndexes().forEach((indexName, index) -> {
                    final String indexTableName = indexTableName(schema, index);
                    final String indexPartition = indexPartitionName(schema, index);
                    final String indexSort = indexSortName(schema, index);

                    if(indexType(schema, index) == IndexType.GSI) {
                        gsis.add(GlobalSecondaryIndexDescription.builder()
                                .indexName(indexTableName)
                                .keySchema(
                                        DynamoDBUtils.keySchemaElement(indexPartition, KeyType.HASH),
                                        DynamoDBUtils.keySchemaElement(indexSort, KeyType.RANGE)
                                )
                                .build());
                        attributeDefinitions.add(DynamoDBUtils.attributeDefinition(indexPartition, ScalarAttributeType.B));
                        attributeDefinitions.add(DynamoDBUtils.attributeDefinition(indexSort, ScalarAttributeType.B));
                    } else {
                        tables.put(schema.getName() + Reserved.PREFIX + index.getName() + Reserved.PREFIX + "Index", TableDescription.builder()
                                .tableName(indexTableName)
                                .keySchema(
                                        DynamoDBUtils.keySchemaElement(indexPartition, KeyType.HASH),
                                        DynamoDBUtils.keySchemaElement(indexSort, KeyType.RANGE)
                                )
                                .attributeDefinitions(
                                        DynamoDBUtils.attributeDefinition(indexPartition, ScalarAttributeType.B),
                                        DynamoDBUtils.attributeDefinition(indexSort, ScalarAttributeType.B)
                                )
                                .build());
                    }
                });

                final String objectTableName = objectTableName(schema);
                final String objectPartition = objectPartitionName(schema);
                attributeDefinitions.add(DynamoDBUtils.attributeDefinition(objectPartition, ScalarAttributeType.S));
                tables.put(schema.getName() + Reserved.PREFIX + "Object", TableDescription.builder()
                        .tableName(objectTableName)
                        .keySchema(DynamoDBUtils.keySchemaElement(objectPartition, KeyType.HASH))
                        .attributeDefinitions(attributeDefinitions)
                        .globalSecondaryIndexes(gsis)
                        .build());

                final String historyTableName = historyTableName(schema);
                final String historyPartition = historyPartitionName(schema);
                final String historySort = historySortName(schema);
                tables.put(schema.getName() + Reserved.PREFIX + "History", TableDescription.builder()
                        .tableName(historyTableName)
                        .keySchema(
                                DynamoDBUtils.keySchemaElement(historyPartition, KeyType.HASH),
                                DynamoDBUtils.keySchemaElement(historySort, KeyType.HASH)
                        )
                        .attributeDefinitions(
                                DynamoDBUtils.attributeDefinition(historyPartition, ScalarAttributeType.S),
                                DynamoDBUtils.attributeDefinition(historySort, ScalarAttributeType.N)
                        )
                        .globalSecondaryIndexes(gsis)
                        .build());
            });

            return tables;
        }
    }
}

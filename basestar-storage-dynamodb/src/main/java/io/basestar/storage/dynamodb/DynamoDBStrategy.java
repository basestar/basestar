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
import io.basestar.schema.*;
import io.basestar.util.Nullsafe;
import lombok.Data;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface DynamoDBStrategy extends Serializable {

    String objectTableName(ReferableSchema schema);

    String historyTableName(ReferableSchema schema);

    String sequenceTableName(SequenceSchema schema);

    String indexTableName(ReferableSchema schema, Index index);

    String objectPartitionPrefix(ReferableSchema schema);

    String historyPartitionPrefix(ReferableSchema schema);

    String indexPartitionPrefix(ReferableSchema schema, Index index);

    String sequencePartitionPrefix(SequenceSchema schema);

    String objectPartitionName(ReferableSchema schema);

    String historyPartitionName(ReferableSchema schema);

    String historySortName(ReferableSchema schema);

    String indexPartitionName(ReferableSchema schema, Index index);

    String sequencePartitionName(SequenceSchema schema);

    String indexSortName(ReferableSchema schema, Index index);

    IndexType indexType(ReferableSchema schema, Index index);


    Map<String, TableDescription> tables(List<? extends ReferableSchema> schemas);

    enum IndexType {

        EXT,
        GSI
    }

    @Data
    class SingleTable implements DynamoDBStrategy {

        public static final String OBJECT_PARTITION_KEY = Reserved.PREFIX + "partition";

        public static final String HISTORY_PARTITION_KEY = OBJECT_PARTITION_KEY;

        public static final String HISTORY_SORT_KEY = ObjectSchema.VERSION;

        public static final String INDEX_PARTITION_KEY = OBJECT_PARTITION_KEY;

        public static final String INDEX_SORT_KEY = Reserved.PREFIX + "sort";

        public static final String SEQUENCE_PARTITION_KEY = OBJECT_PARTITION_KEY;

        private final String tablePrefix;

        private final String dataPrefix;

        @lombok.Builder(builderClassName = "Builder")
        protected SingleTable(final String tablePrefix, final String dataPrefix) {

            this.tablePrefix = Nullsafe.orDefault(tablePrefix);
            this.dataPrefix = Nullsafe.orDefault(dataPrefix);
        }

        @Override
        public String objectPartitionPrefix(final ReferableSchema schema) {

            return dataPrefix + schema.getQualifiedName() + Reserved.DELIMITER + schema.getVersion();
        }

        @Override
        public String historyPartitionPrefix(final ReferableSchema schema) {

            return objectPartitionPrefix(schema);
        }

        @Override
        public String indexPartitionPrefix(final ReferableSchema schema, final Index index) {

            return  dataPrefix + schema.getQualifiedName() + Reserved.DELIMITER + schema.getVersion()
                    + Reserved.DELIMITER + index.getName() + Reserved.DELIMITER + index.getVersion();
        }

        @Override
        public String objectPartitionName(final ReferableSchema schema) {

            return OBJECT_PARTITION_KEY;
        }

        @Override
        public String historyPartitionName(final ReferableSchema schema) {

            return HISTORY_PARTITION_KEY;
        }

        @Override
        public String historySortName(final ReferableSchema schema) {

            return HISTORY_SORT_KEY;
        }

        @Override
        public String indexPartitionName(final ReferableSchema schema, final Index index) {

            return INDEX_PARTITION_KEY;
        }

        @Override
        public String indexSortName(final ReferableSchema schema, final Index index) {

            return INDEX_SORT_KEY;
        }

        @Override
        public String sequencePartitionPrefix(final SequenceSchema schema) {

            return dataPrefix + schema.getQualifiedName() + Reserved.DELIMITER + schema.getVersion();
        }

        @Override
        public String sequencePartitionName(final SequenceSchema schema) {

            return SEQUENCE_PARTITION_KEY;
        }

        @Override
        public IndexType indexType(final ReferableSchema schema, final Index index) {

            return IndexType.EXT;
        }

        @Override
        public String objectTableName(final ReferableSchema schema) {

            return tablePrefix + objectTableName();
        }

        @Override
        public String historyTableName(final ReferableSchema schema) {

            return tablePrefix + historyTableName();
        }

        @Override
        public String indexTableName(final ReferableSchema schema, final Index index) {

            return tablePrefix + indexTableName();
        }

        @Override
        public String sequenceTableName(final SequenceSchema schema) {

            return tablePrefix + sequenceTableName();
        }

        public String objectTableName() {

            return "Object";
        }

        public String historyTableName() {

            return "History";
        }

        public String indexTableName() {

            return "Index";
        }

        public String sequenceTableName() {

            return "Sequence";
        }

        @Override
        public Map<String, TableDescription> tables(final List<? extends ReferableSchema> schemas) {

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
                            .build(),
                    "Sequence", TableDescription.builder()
                            .tableName(sequenceTableName(null))
                            .keySchema(
                                    DynamoDBUtils.keySchemaElement(SEQUENCE_PARTITION_KEY, KeyType.HASH)
                            )
                            .attributeDefinitions(
                                    DynamoDBUtils.attributeDefinition(SEQUENCE_PARTITION_KEY, ScalarAttributeType.S)
                            )
                            .build()
            );
        }
    }

    @Data
    class MultiTable implements DynamoDBStrategy {

        public static final String NAME_DELIMITER = ".";

        public static final String OBJECT_PARTITION_KEY = ObjectSchema.ID;

        public static final String HISTORY_PARTITION_KEY = OBJECT_PARTITION_KEY;

        public static final String HISTORY_SORT_KEY = ObjectSchema.VERSION;

        public static final String EXT_INDEX_PARTITION_KEY = Reserved.PREFIX + "partition";

        public static final String EXT_INDEX_SORT_KEY = Reserved.PREFIX + "sort";

        public static final String SEQUENCE_PARTITION_KEY = ReferableSchema.SCHEMA;

        private final String tablePrefix;

        @lombok.Builder(builderClassName = "Builder")
        MultiTable(final String tablePrefix) {

            this.tablePrefix = tablePrefix;
        }

        @Override
        public String objectPartitionPrefix(final ReferableSchema schema) {

            return null;
        }

        @Override
        public String historyPartitionPrefix(final ReferableSchema schema) {

            return null;
        }

        @Override
        public String indexPartitionPrefix(final ReferableSchema schema, final Index index) {

            return null;
        }

        @Override
        public String sequencePartitionPrefix(final SequenceSchema schema) {

            return null;
        }

        @Override
        public String objectPartitionName(final ReferableSchema schema) {

            return OBJECT_PARTITION_KEY;
        }

        @Override
        public String historyPartitionName(final ReferableSchema schema) {

            return HISTORY_PARTITION_KEY;
        }

        @Override
        public String historySortName(final ReferableSchema schema) {

            return HISTORY_PARTITION_KEY;
        }

        @Override
        public String indexPartitionName(final ReferableSchema schema, final Index index) {

            switch (indexType(schema, index)) {
                case EXT:
                    return EXT_INDEX_PARTITION_KEY;
                case GSI:
                    return gsiPartitionName(index);
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public String sequencePartitionName(final SequenceSchema schema) {

            return SEQUENCE_PARTITION_KEY;
        }

        @Override
        public String indexSortName(final ReferableSchema schema, final Index index) {

            switch (indexType(schema, index)) {
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
        public String objectTableName(final ReferableSchema schema) {

            return tablePrefix + schema.getQualifiedName() + NAME_DELIMITER + schema.getVersion();
        }

        @Override
        public String sequenceTableName(final SequenceSchema schema) {

            return tablePrefix + schema.getQualifiedName() + NAME_DELIMITER + schema.getVersion();
        }

        @Override
        public String historyTableName(final ReferableSchema schema) {

            return tablePrefix + schema.getQualifiedName() + NAME_DELIMITER + schema.getVersion()
                    + NAME_DELIMITER + "History";
        }

        @Override
        public String indexTableName(final ReferableSchema schema, final Index index) {

            switch (indexType(schema, index)) {
                case EXT:
                    return tablePrefix + schema.getQualifiedName() + NAME_DELIMITER + schema.getVersion()
                            + NAME_DELIMITER + index.getName() + NAME_DELIMITER + index.getVersion();
                case GSI:
                    return index.getName() + NAME_DELIMITER + index.getVersion();
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public IndexType indexType(final ReferableSchema schema, final Index index) {

            if(index.getConsistency() == Consistency.EVENTUAL) {
                return IndexType.GSI;
            } else {
                return IndexType.EXT;
            }
        }

        @Override
        public Map<String, TableDescription> tables(final List<? extends ReferableSchema> schemas) {

            final Map<String, TableDescription> tables = new HashMap<>();

            schemas.forEach(schema -> {

                final List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
                final List<GlobalSecondaryIndexDescription> gsis = new ArrayList<>();
                schema.getIndexes().forEach((indexName, index) -> {
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
                        tables.put(schema.getQualifiedName() + Reserved.PREFIX + index.getName() + Reserved.PREFIX + "Index", TableDescription.builder()
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
                tables.put(schema.getQualifiedName() + Reserved.PREFIX + "Object", TableDescription.builder()
                        .tableName(objectTableName)
                        .keySchema(DynamoDBUtils.keySchemaElement(objectPartition, KeyType.HASH))
                        .attributeDefinitions(attributeDefinitions)
                        .globalSecondaryIndexes(gsis)
                        .build());

                final String historyTableName = historyTableName(schema);
                final String historyPartition = historyPartitionName(schema);
                final String historySort = historySortName(schema);
                tables.put(schema.getQualifiedName() + Reserved.PREFIX + "History", TableDescription.builder()
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

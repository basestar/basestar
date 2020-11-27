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

import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStash;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.test.Localstack;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;

@Slf4j
class TestDynamoDBStorage extends TestStorage {

    private static final int WAIT_ATTEMPTS = 10;

    private static final int WAIT_DELAY_MILLIS = 2000;

    private static DynamoDbAsyncClient ddb;

    private final List<String> tableNames = new ArrayList<>();

    @BeforeAll
    static void startLocalStack() {

        Localstack.startDynamoDB();
        ddb = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create(Localstack.DDB_ENDPOINT))
                .build();
    }

    @AfterAll
    static void close() {

        if(ddb != null) {
            ddb.close();
            ddb = null;
        }
    }

    @AfterEach
    void cleanup() {

        // If we don't do this, localstack eventually falls over
        tableNames.forEach(tableName -> ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()));
    }

    @Override
    protected Storage storage(final Namespace namespace) {

        final DynamoDBStrategy ddbRouting = DynamoDBStrategy.SingleTable.builder().tablePrefix(UUID.randomUUID() + "-").build();
        final Storage storage = DynamoDBStorage.builder().setClient(ddb).setStrategy(ddbRouting)
                .setEventStrategy(Storage.EventStrategy.SUPPRESS)
                .setOversizeStash(MemoryStash.builder().build())
                .build();

        for(final TableDescription table : ddbRouting.tables(Collections.emptyList()).values()) {
            ddb.createTable(DynamoDBUtils.createTableRequest(table)).join();
            waitUntilTableReady(ddb, table.tableName());
            tableNames.add(table.tableName());
        }

        return storage;
    }

    private static void waitUntilTableReady(final DynamoDbAsyncClient ddb, final String tableName) {

        for(int i = 0; i != WAIT_ATTEMPTS; ++i) {
            try {
                final DescribeTableResponse check = ddb.describeTable(DescribeTableRequest.builder()
                        .tableName(tableName)
                        .build()).join();
                if (check.table().tableStatus() == TableStatus.ACTIVE) {
                    return;
                }
            } catch (final CompletionException e) {
                if (!(e.getCause() instanceof ResourceNotFoundException)) {
                    throw e;
                }
            }
            log.info("Table {} not stable, waiting for {} millis", tableName, WAIT_DELAY_MILLIS);
            try {
                Thread.sleep(WAIT_DELAY_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new IllegalStateException("Table " + tableName + " never stabilised");
    }

    @Override
    protected void testNullBeforeDelete() {

    }

    @Override
    protected void testNullBeforeUpdate() {

    }

    @Override
    protected boolean supportsRepair() {

        return true;
    }
}

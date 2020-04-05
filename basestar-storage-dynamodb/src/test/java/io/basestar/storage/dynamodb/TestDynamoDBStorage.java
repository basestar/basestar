package io.basestar.storage.dynamodb;

import com.google.common.collect.Multimap;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStash;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.test.Localstack;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;

@Slf4j
public class TestDynamoDBStorage extends TestStorage {

    private static final int WAIT_ATTEMPTS = 10;

    private static final int WAIT_DELAY_MILLIS = 2000;

    @BeforeAll
    public static void startLocalStack() {

        Localstack.start();
    }

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create(Localstack.DDB_ENDPOINT))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(ddb::close));

        final DynamoDBRouting ddbRouting = new DynamoDBRouting.SingleTable(UUID.randomUUID() + "-");
        final Storage storage = DynamoDBStorage.builder().setClient(ddb).setRouting(ddbRouting)
                .setEventStrategy(Storage.EventStrategy.SUPPRESS)
                .setOversizeStash(MemoryStash.builder().build())
                .build();

        for(final TableDescription table : ddbRouting.tables(Collections.emptyList()).values()) {
            ddb.createTable(DynamoDBUtils.createTableRequest(table)).join();
            waitUntilTableReady(ddb, table.tableName());
        }

        writeAll(storage, namespace, data);

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
    public void testNullBeforeDelete() {

    }

    @Override
    public void testNullBeforeUpdate() {

    }
}

package io.basestar.storage.s3;

import com.google.common.collect.Multimap;
import io.basestar.schema.Namespace;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.test.Localstack;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class TestS3BlobStorage extends TestStorage {

    @BeforeAll
    public static void startLocalStack() {

        Localstack.start();
    }

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final S3AsyncClient client = S3AsyncClient.builder()
                .endpointOverride(URI.create(Localstack.S3_ENDPOINT))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(client::close));

        final S3BlobRouting.Simple routing = new S3BlobRouting.Simple(UUID.randomUUID().toString(), "test/");

        client.createBucket(CreateBucketRequest.builder().bucket(routing.getBucket()).build()).join();

        final Storage storage = S3BlobStorage.builder()
                .setClient(client).setRouting(routing)
                .build();

        writeAll(storage, namespace, data);

        return storage;
    }
}

package io.basestar.connector.dynamodb;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.basestar.database.event.ObjectCreatedEvent;
import io.basestar.database.event.ObjectDeletedEvent;
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.event.Emitter;
import io.basestar.event.EventSerialization;
import io.basestar.event.sns.SNSEmitter;
import io.basestar.schema.Instance;
import io.basestar.storage.Stash;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import io.basestar.storage.dynamodb.DynamoDBUtils;
import io.basestar.storage.s3.S3Stash;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DynamoDBStreamHandler implements RequestStreamHandler {

    public static final String TARGET_TYPE = "TARGET_TYPE";

    public static final String TARGET_TOPIC_ARN = "TARGET_TOPIC_ARN";

    public static final String TARGET_OVERSIZE_TYPE = "TARGET_OVERSIZE_TYPE";

    public static final String TARGET_OVERSIZE_BUCKET = "TARGET_OVERSIZE_BUCKET";

    public static final String TARGET_OVERSIZE_PREFIX = "TARGET_OVERSIZE_PREFIX";

    public static final String SOURCE_OVERSIZE_TYPE = "SOURCE_OVERSIZE_TYPE";

    public static final String SOURCE_OVERSIZE_BUCKET = "SOURCE_OVERSIZE_BUCKET";

    public static final String SOURCE_OVERSIZE_PREFIX = "SOURCE_OVERSIZE_PREFIX";

    private final Emitter target;

    private final Stash sourceOversize;

    private final ObjectMapper objectMapper;

    public DynamoDBStreamHandler(final Emitter target, final Stash sourceOversize) {

        this.target = target;
        this.sourceOversize = sourceOversize;
        this.objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    public DynamoDBStreamHandler() {

        this(createTarget(), createSourceOversize());
    }

    private static Emitter createTarget() {

        final String targetType = System.getenv(TARGET_TYPE);
        switch (targetType.toUpperCase()) {
            case "SNS": {
                final SnsAsyncClient sns = SnsAsyncClient.builder().build();
                final String topicArn = System.getenv(TARGET_TOPIC_ARN);
                return SNSEmitter.builder()
                        .setClient(sns)
                        .setTopicArn(topicArn)
                        .setOversizeStash(createTargetOversize())
                        .setSerialization(EventSerialization.gzipBson())
                        .build();
            }
            case "SKIP": {
                return new Emitter.Skip();
            }
            default:
                throw new IllegalStateException();
        }
    }

    private static Stash createTargetOversize() {

        final String oversizeType = System.getenv(TARGET_OVERSIZE_TYPE);
        if(oversizeType == null) {
            return null;
        }
        switch (oversizeType.toUpperCase()) {
            case "S3": {
                final S3AsyncClient s3 = S3AsyncClient.builder().build();
                final String bucket = System.getenv(TARGET_OVERSIZE_BUCKET);
                final String prefix = System.getenv(TARGET_OVERSIZE_PREFIX);
                return S3Stash.builder().setClient(s3)
                        .setBucket(bucket).setPrefix(prefix)
                        .build();
            }
            default:
                throw new IllegalStateException();
        }
    }

    private static Stash createSourceOversize() {

        final String oversizeType = System.getenv(SOURCE_OVERSIZE_TYPE);
        if(oversizeType == null) {
            return null;
        }
        switch (oversizeType.toUpperCase()) {
            case "S3": {
                final S3AsyncClient s3 = S3AsyncClient.builder().build();
                final String bucket = System.getenv(SOURCE_OVERSIZE_BUCKET);
                final String prefix = System.getenv(SOURCE_OVERSIZE_PREFIX);
                return S3Stash.builder().setClient(s3)
                        .setBucket(bucket).setPrefix(prefix)
                        .build();
            }
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void handleRequest(final InputStream is, final OutputStream os, final Context context) throws IOException {

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        final DynamoDBEvent event = objectMapper.readValue(is, DynamoDBEvent.class);
        for(final DynamoDBEvent.Record record : event.getRecords()) {
            futures.add(handleRecord(record));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    }

    private CompletableFuture<?> handleRecord(final DynamoDBEvent.Record record) {

        switch (record.getEventName()) {
            case INSERT:
                return handleInsert(record);
            case MODIFY:
                return handleModify(record);
            case REMOVE:
                return handleRemove(record);
            default:
                throw new IllegalStateException();
        }
    }

    private CompletableFuture<Map<String, Object>> checkOversize(final Map<String, Object> object) {

        final String oversizeKey = DynamoDBStorage.checkOversize(object);
        if(oversizeKey != null) {
            if (sourceOversize != null) {
                return sourceOversize.read(oversizeKey)
                        .thenApply(DynamoDBUtils::fromOversizeBytes);
            } else {
                throw new IllegalStateException("Oversize object encountered without oversize stash");
            }
        } else {
            return CompletableFuture.completedFuture(object);
        }
    }

    private CompletableFuture<?> handleInsert(final DynamoDBEvent.Record record) {

        final StreamRecord streamRecord = record.getDynamodb();
        return checkOversize(DynamoDBUtils.fromItem(streamRecord.newImage()))
                .thenCompose(after -> {
                    final String schema = Instance.getSchema(after);
                    final String id = Instance.getId(after);
                    return target.emit(ObjectCreatedEvent.of(schema, id, after));
                });
    }

    private CompletableFuture<?> handleModify(final DynamoDBEvent.Record record) {

        final StreamRecord streamRecord = record.getDynamodb();
        return checkOversize(DynamoDBUtils.fromItem(streamRecord.oldImage()))
                .thenCompose(before -> checkOversize(DynamoDBUtils.fromItem(streamRecord.newImage()))
                        .thenCompose(after -> {
                            final String schema = Instance.getSchema(before);
                            final String id = Instance.getId(before);
                            final Long version = Instance.getVersion(before);
                            assert version != null;
                            return target.emit(ObjectUpdatedEvent.of(schema, id, version, before, after));
                        }));
    }

    private CompletableFuture<?> handleRemove(final DynamoDBEvent.Record record) {

        final StreamRecord streamRecord = record.getDynamodb();
        return checkOversize(DynamoDBUtils.fromItem(streamRecord.oldImage()))
                .thenCompose(before -> {
                    final String schema = Instance.getSchema(before);
                    final String id = Instance.getId(before);
                    final Long version = Instance.getVersion(before);
                    assert version != null;
                    return target.emit(ObjectDeletedEvent.of(schema, id, version, before));
                });
    }
}

package io.basestar.storage.aws.stepfunction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.ViewSchema;
import io.basestar.storage.*;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.util.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.*;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

@Slf4j
public class StepFunctionStorage implements DefaultLayerStorage {

    // An execution object is at version 1 if running, 2 if complete
    private static final long MIN_VERSION = 1L;

    private static final long MAX_VERSION = 2L;

    private final SfnAsyncClient client;

    private final StepFunctionStrategy strategy;

    private final ObjectMapper payloadMapper;

    @lombok.Builder(builderClassName = "Builder")
    protected StepFunctionStorage(final SfnAsyncClient client,
                                  final StepFunctionStrategy strategy,
                                  final ObjectMapper payloadMapper) {

        this.client = client;
        this.strategy = strategy;
        this.payloadMapper = Nullsafe.orDefault(payloadMapper, () -> new ObjectMapper().registerModule(BasestarModule.INSTANCE));
    }

    private CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        return readImpl(schema, id);
    }

    private CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

        if(version >= MIN_VERSION && version <= MAX_VERSION) {
            return readImpl(schema, id, version);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private String executionArn(final String stateMachineArn, final String id) {

        final String[] inputParts = stateMachineArn.split(":");
        assert inputParts.length == 7;
        final String region = inputParts[3];
        final String accountId = inputParts[4];
        final String name = inputParts[6];
        final String[] outputParts = {
                "arn", "aws", "states", region, accountId, "execution", name, id
        };
        return Joiner.on(":").join(outputParts);
    }

    private CompletableFuture<Map<String, Object>> readImpl(final ReferableSchema schema, final String id) {

        return readImpl(schema, id, MAX_VERSION);
    }

    private CompletableFuture<Map<String, Object>> readImpl(final ReferableSchema schema, final String id, final long maxVersion) {

        final String stepFunctionArn = strategy.stateMachineArn(schema);
        return client.describeExecution(DescribeExecutionRequest.builder()
                .executionArn(executionArn(stepFunctionArn, id))
                .build()).thenApply(v -> toResponse(schema, v, maxVersion));
    }

    private Map<String, Object> toResponse(final ReferableSchema schema, final DescribeExecutionResponse response, final long maxVersion) {

        final String id = response.name();
        final Instant created = response.startDate();
        final Instant updated;
        final ExecutionStatus status;
        final long version;
        if(maxVersion == MIN_VERSION) {
            status = ExecutionStatus.RUNNING;
            updated = created;
            version = MIN_VERSION;
        } else {
            status = response.status();
            updated = Nullsafe.orDefault(response.stopDate(), created);
            version = isComplete(status) ? MAX_VERSION : MIN_VERSION;
        }
        final Map<String, Object> data = new HashMap<>(parsePayload(response.input()));
        if(maxVersion == MAX_VERSION) {
            data.putAll(parsePayload(response.output()));
        }
        data.put(ObjectSchema.ID, id);
        data.put(ObjectSchema.CREATED, created);
        data.put(ObjectSchema.UPDATED, updated);
        data.put(ObjectSchema.VERSION, version);
        data.put(strategy.statusProperty(schema), strategy.statusValue(schema, status));
        return data;
    }

    private boolean isComplete(final ExecutionStatus status) {

        return status != ExecutionStatus.RUNNING;
    }

    private Map<String, Object> parsePayload(final String payload) {

        try {
            if(payload == null) {
                return ImmutableMap.of();
            } else {
                return payloadMapper.readValue(payload, new TypeReference<Map<String, Object>>() {
                });
            }
        } catch (final IOException e) {
            log.error("Failed to parse payload", e);
            return ImmutableMap.of();
        }
    }

    private String printPayload(final Map<String, Object> payload) {

        try {
            return payloadMapper.writeValueAsString(payload);
        } catch (final IOException e) {
            log.error("Failed to print payload", e);
            return "{}";
        }
    }

    private CompletableFuture<Page<Map<String, Object>>> toResponse(final ObjectSchema schema, final ListExecutionsResponse response) {

        return CompletableFutures.allOf(response.executions().stream()
                .map(e -> readImpl(schema, e.name()))
                .collect(Collectors.toList()))
                .thenApply(results -> new Page<>(results, toResponseToken(response.nextToken())));
    }

    private static Page.Token toResponseToken(final String paging) {

        return paging == null ? null : Page.Token.fromStringValue(paging);
    }

    private static String toRequestToken(final Page.Token paging) {

        return paging == null ? null : paging.getStringValue();
    }

    @Override
    public Pager<Map<String, Object>> queryObject(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final String stepFunctionArn = strategy.stateMachineArn(schema);
        return (stats, paging, count) -> client.listExecutions(ListExecutionsRequest.builder()
            .stateMachineArn(stepFunctionArn)
            .nextToken(toRequestToken(paging))
            .maxResults(count)
            .build()).thenCompose(v -> toResponse(schema, v));
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final BatchCapture capture = new BatchCapture();

            @Override
            public ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                capture.captureLatest(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                capture.captureVersion(schema, id, version, expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                final Map<BatchResponse.RefKey, CompletableFuture<Map<String, Object>>> futures = new HashMap<>();
                capture.forEachRef((schema, key, args) -> {
                    if(key.hasVersion()) {
                        futures.put(key, readImpl(schema, key.getId(), key.getVersion()));
                    } else {
                        futures.put(key, readImpl(schema, key.getId()));
                    }
                });
                return CompletableFutures.allOf(futures).thenApply(BatchResponse::new);
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            final Map<BatchResponse.RefKey, CreateAction> creates = new HashMap<>();

            @RequiredArgsConstructor
            class CreateAction {

                private final ReferableSchema schema;

                private final String id;

                private final Map<String, Object> after;

                public CompletableFuture<Map<String, Object>> apply() {

                    return client.startExecution(StartExecutionRequest.builder()
                            .stateMachineArn(strategy.stateMachineArn(schema))
                            .name(id)
                            .input(printPayload(after))
                            .build())
                            .thenCompose(v -> readImpl(schema, id, MIN_VERSION))
                            .exceptionally(e -> {
                                if(e.getCause() instanceof ExecutionAlreadyExistsException) {
                                    throw new ObjectExistsException(schema.getQualifiedName(), id);
                                } else {
                                    throw new CompletionException(e.getCause());
                                }
                            });
                }
            }

            @Override
            public StorageTraits storageTraits(final ReferableSchema schema) {

                return StepFunctionStorage.this.storageTraits(schema);
            }

            @Override
            public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                final BatchResponse.RefKey key = BatchResponse.RefKey.version(schema.getQualifiedName(), id, MIN_VERSION);
                creates.put(key, new CreateAction(schema, id, after));
            }

            @Override
            public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

                throw new UnsupportedOperationException();
            }

            @Override
            public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

//                throw new UnsupportedOperationException();
            }

            @Override
            public Storage.WriteTransaction writeView(final ViewSchema schema, final Map<String, Object> before, final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                final Map<BatchResponse.RefKey, CompletableFuture<Map<String, Object>>> futures = creates.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().apply()
                ));

                return CompletableFutures.allOf(futures).thenApply(BatchResponse::new);
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return strategy.eventStrategy(schema);
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return StepFunctionStorageTraits.INSTANCE;
    }
}

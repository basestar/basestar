package io.basestar.storage.aws.stepfunction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.Versioning;
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
public class StepFunctionStorage implements Storage, Storage.WithoutAggregate,
        Storage.WithoutWriteIndex, Storage.WithoutExpand, Storage.WithoutRepair {

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

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        return readImpl(schema, id);
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

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

    private CompletableFuture<Map<String, Object>> readImpl(final ObjectSchema schema, final String id) {

        return readImpl(schema, id, MAX_VERSION);
    }

    private CompletableFuture<Map<String, Object>> readImpl(final ObjectSchema schema, final String id, final long maxVersion) {

        final String stepFunctionArn = strategy.stateMachineArn(schema);
        return client.describeExecution(DescribeExecutionRequest.builder()
                .executionArn(executionArn(stepFunctionArn, id))
                .build()).thenApply(v -> toResponse(schema, v, maxVersion));
    }

    private Map<String, Object> toResponse(final ObjectSchema schema, final DescribeExecutionResponse response, final long maxVersion) {

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
    public List<Pager.Source<Map<String, Object>>> queryObject(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final String stepFunctionArn = strategy.stateMachineArn(schema);
        return ImmutableList.of(
                (count, paging, stats) -> client.listExecutions(ListExecutionsRequest.builder()
                        .stateMachineArn(stepFunctionArn)
                        .nextToken(toRequestToken(paging))
                        .maxResults(count)
                        .build()).thenCompose(v -> toResponse(schema, v))
        );
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            final Map<BatchResponse.Key, CreateAction> creates = new HashMap<>();

            @RequiredArgsConstructor
            class CreateAction {

                private final ObjectSchema schema;

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
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                final BatchResponse.Key key = BatchResponse.Key.version(schema.getQualifiedName(), id, MIN_VERSION);
                creates.put(key, new CreateAction(schema, id, after));
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                final Map<BatchResponse.Key, CompletableFuture<Map<String, Object>>> futures = creates.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().apply()
                ));

                return CompletableFutures.allOf(futures).thenApply(BatchResponse.Basic::new);
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return strategy.eventStrategy(schema);
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return StepFunctionStorageTraits.INSTANCE;
    }

    @Override
    public CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

        throw new UnsupportedOperationException();
    }
}

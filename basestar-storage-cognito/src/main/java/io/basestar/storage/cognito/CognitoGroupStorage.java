package io.basestar.storage.cognito;

/*-
 * #%L
 * basestar-storage-cognito
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

import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.util.*;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderAsyncClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class CognitoGroupStorage implements DefaultLayerStorage {

    private static final String DESCRIPTION_KEY = "description";

    private static final int MAX_PAGE_SIZE = 60;

    private final CognitoIdentityProviderAsyncClient client;

    private final CognitoGroupStrategy strategy;

    private CognitoGroupStorage(final Builder builder) {

        this.client = builder.client;
        this.strategy = builder.strategy;
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private CognitoIdentityProviderAsyncClient client;

        private CognitoGroupStrategy strategy;

        public CognitoGroupStorage build() {

            return new CognitoGroupStorage(this);
        }
    }

    @Override
    public Pager<Map<String, Object>> queryObject(final Consistency consistency, final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return (stats, token, count) -> {
            final String userPoolId = strategy.getUserPoolId(schema);
            return client.listGroups(ListGroupsRequest.builder()
                    .userPoolId(userPoolId)
                    .limit(Math.max(MAX_PAGE_SIZE, count))
                    .nextToken(decodePaging(token))
                    .build()).thenApply(response -> {
                final List<GroupType> groups = response.groups();
                return new Page<>(groups.stream().map(this::fromGroup)
                        .collect(Collectors.toList()), encodePaging(response.nextToken()));

            });
        };
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        throw new UnsupportedOperationException();
    }

    private String decodePaging(final Page.Token token) {

        return Nullsafe.map(token, v -> new String(v.getValue(), StandardCharsets.UTF_8));
    }

    private Page.Token encodePaging(final String token) {

        return Nullsafe.map(token, v -> new Page.Token(token.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            final BatchCapture capture = new BatchCapture();

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
                    final String userPoolId = strategy.getUserPoolId(schema);
                    futures.put(key, client.getGroup(GetGroupRequest.builder()
                            .userPoolId(userPoolId)
                            .groupName(key.getId())
                            .build())
                            .thenApply(response -> key.matchOrNull(fromGroup(response.group())))
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof ResourceNotFoundException) {
                                    log.warn("Group {} not found", key.getId());
                                    return null;
                                } else if(cause instanceof RuntimeException) {
                                    throw (RuntimeException)e.getCause();
                                } else {
                                    throw new IllegalStateException(cause);
                                }
                            }));
                });
                return CompletableFutures.allOf(futures).thenApply(BatchResponse::new);
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            private final List<Supplier<CompletableFuture<BatchResponse>>> requests = new ArrayList<>();

            @Override
            public StorageTraits storageTraits(final ReferableSchema schema) {

                return CognitoGroupStorage.this.storageTraits(schema);
            }

            @Override
            public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    final String description = null;
                    return client.createGroup(CreateGroupRequest.builder()
                            .userPoolId(userPoolId)
                            .groupName(id)
                            .description(description)
                            .build())
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof GroupExistsException) {
                                    throw new ObjectExistsException(schema.getQualifiedName(), id);
                                } else if(cause instanceof RuntimeException) {
                                    throw (RuntimeException)e.getCause();
                                } else {
                                    throw new IllegalStateException(cause);
                                }
                            })
                            .thenApply(ignored -> BatchResponse.fromRef(schema.getQualifiedName(), after));
                });
            }

            @Override
            public void updateObjectLayer(final ReferableSchema schema,final String id, final Map<String, Object> before, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    final String description = null;
                    return client.updateGroup(UpdateGroupRequest.builder()
                            .userPoolId(userPoolId)
                            .groupName(id)
                            .description(description)
                            .build())
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof ResourceNotFoundException) {
                                    log.warn("User {} not found", id);
                                    throw new VersionMismatchException(schema.getQualifiedName(), id, Instance.getVersion(before));
                                } else if(cause instanceof RuntimeException) {
                                    throw (RuntimeException)e.getCause();
                                } else {
                                    throw new IllegalStateException(cause);
                                }
                            })
                            .thenApply(ignored -> BatchResponse.fromRef(schema.getQualifiedName(), after));
                });
            }

            @Override
            public void deleteObjectLayer(final ReferableSchema schema,final String id, final Map<String, Object> before) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    return client.deleteGroup(DeleteGroupRequest.builder()
                            .userPoolId(userPoolId)
                            .groupName(id)
                            .build())
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof ResourceNotFoundException) {
                                    log.warn("Group {} not found", id);
                                    throw new VersionMismatchException(schema.getQualifiedName(), id, Instance.getVersion(before));
                                } else if(cause instanceof RuntimeException) {
                                    throw (RuntimeException)e.getCause();
                                } else {
                                    throw new IllegalStateException(cause);
                                }
                            })
                            .thenApply(ignored -> BatchResponse.empty());
                });
            }

            @Override
            public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

//                throw new UnsupportedOperationException("cannot write history");
            }

            @Override
            public Storage.WriteTransaction write(final LinkableSchema schema,  final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return BatchResponse.mergeFutures(requests.stream().map(Supplier::get));
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return CognitoStorageTraits.INSTANCE;
    }

    public String getDescription(final Map<String, Object> data) {

        return (String)data.get(DESCRIPTION_KEY);
    }

    private Map<String, Object> fromGroup(final GroupType group) {

        final Map<String, Object> result = new HashMap<>();
        result.put(ReferableSchema.ID, group.groupName());
        result.put(ReferableSchema.VERSION, 1L);
        result.put(DESCRIPTION_KEY, group.description());
        return result;
    }
}

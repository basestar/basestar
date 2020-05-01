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

import com.google.common.collect.ImmutableList;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderAsyncClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CognitoUserStorage implements Storage {

    private final CognitoIdentityProviderAsyncClient client;

    private final CognitoUserRouting routing;

    private CognitoUserStorage(final Builder builder) {

        this.client = builder.client;
        this.routing = builder.routing;
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private CognitoIdentityProviderAsyncClient client;

        private CognitoUserRouting routing;

        public CognitoUserStorage build() {

            return new CognitoUserStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final String userPoolId = routing.getUserPoolId(schema);
        return client.adminGetUser(AdminGetUserRequest.builder()
                .userPoolId(userPoolId)
                .username(id)
                .build())
                .thenApply(v -> fromResponse(schema, v));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        throw new UnsupportedOperationException();
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        return ImmutableList.of(
                (count, token) -> {
                    final String userPoolId = routing.getUserPoolId(schema);
                    return client.listUsers(ListUsersRequest.builder()
                            .userPoolId(userPoolId)
                            .limit(count)
                            .paginationToken(decodePaging(token))
                            .build()).thenApply(response -> {
                        final List<UserType> users = response.users();
                        return new PagedList<>(users.stream().map(v -> fromUser(schema, v))
                                .collect(Collectors.toList()), encodePaging(response.paginationToken()));
                    });
                });
    }

    private String decodePaging(final PagingToken token) {

        return null;
    }

    private PagingToken encodePaging(final String s) {

        return null;
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return new WriteTransaction() {

            private final List<Supplier<CompletableFuture<BatchResponse>>> requests = new ArrayList<>();

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = routing.getUserPoolId(schema);
                    final List<AttributeType> attributes = attributes(schema, after);
                    return client.adminCreateUser(AdminCreateUserRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .userAttributes(attributes)
                            .build())
                            .thenApply(ignored -> BatchResponse.single(schema.getName(), after));
                });
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = routing.getUserPoolId(schema);
                    final List<AttributeType> attributes = attributes(schema, after);
                    return client.adminUpdateUserAttributes(AdminUpdateUserAttributesRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .userAttributes(attributes)
                            .build())
                            .thenApply(ignored -> BatchResponse.single(schema.getName(), after));
                });
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                requests.add(() -> {
                    final String userPoolId = routing.getUserPoolId(schema);
                    return client.adminDeleteUser(AdminDeleteUserRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .build())
                            .thenApply(ignored -> BatchResponse.empty());
                });
                return this;
            }

            @Override
            public WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<BatchResponse> commit() {

                return BatchResponse.mergeFutures(requests.stream().map(Supplier::get));
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return CognitoStorageTraits.INSTANCE;
    }

    private List<AttributeType> attributes(final ObjectSchema schema, final Map<String, Object> after) {

        final List<AttributeType> result = new ArrayList<>();
        for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
            final String name = entry.getKey();
            final String value = Objects.toString(after.get(name));
            result.add(AttributeType.builder().name(name).value(value).build());
        }
        return result;
    }

    private Map<String, Object> fromUser(final ObjectSchema schema, final UserType user) {

        return from(schema, user.username(), user.attributes(), user.enabled(), user.userStatus(),
                user.userCreateDate(), user.userLastModifiedDate());
    }

    private Map<String, Object> fromResponse(final ObjectSchema schema, final AdminGetUserResponse user) {

        return from(schema, user.username(), user.userAttributes(), user.enabled(), user.userStatus(),
                user.userCreateDate(), user.userLastModifiedDate());
    }

    private Map<String, Object> from(final ObjectSchema schema, final String username,
                                     final List<AttributeType> attributes, final boolean enabled,
                                     final UserStatusType userStatus, final Instant created, final Instant updated) {

        final Map<String, Object> result = new HashMap<>();
        Instance.setSchema(result, schema.getName());
        Instance.setId(result, username);
        if(created != null) {
            Instance.setCreated(result, LocalDateTime.ofInstant(created, ZoneOffset.UTC));
        }
        if(updated != null) {
            Instance.setUpdated(result, LocalDateTime.ofInstant(updated, ZoneOffset.UTC));
        }
        attributes.forEach(attr -> result.put(attr.name(), attr.value()));
        if(userStatus != null) {
            result.put("status", userStatus.toString());
        }
        result.put("enabled", enabled);
        return result;
    }
}

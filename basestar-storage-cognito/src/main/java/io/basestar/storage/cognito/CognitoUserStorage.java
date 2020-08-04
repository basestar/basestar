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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Property;
import io.basestar.schema.use.*;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.Versioning;
import io.basestar.storage.util.Pager;
import io.basestar.util.Name;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderAsyncClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class CognitoUserStorage implements Storage.WithoutWriteIndex, Storage.WithoutHistory, Storage.WithoutAggregate, Storage.WithoutExpand {

    private static final String CUSTOM_ATTR_PREFIX = "custom:";

    private static final Set<String> REQUIRED_ATTRS = ImmutableSet.of(
            "address", "birthdate", "email", "family_name", "gender", "given_name", "locale", "middle_name",
            "name", "nickname", "phone_number", "picture", "preferred_username", "profile", "updated_at",
            "username", "website", "zoneinfo"
    );

    private final CognitoIdentityProviderAsyncClient client;

    private final CognitoUserStrategy strategy;

    private CognitoUserStorage(final Builder builder) {

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

        private CognitoUserStrategy strategy;

        public CognitoUserStorage build() {

            return new CognitoUserStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        final String userPoolId = strategy.getUserPoolId(schema);
        return client.adminGetUser(AdminGetUserRequest.builder()
                .userPoolId(userPoolId)
                .username(id)
                .build())
                .thenApply(v -> fromResponse(schema, v))
                .exceptionally(e -> {
                    final Throwable cause = e.getCause();
                    if(cause instanceof UserNotFoundException) {
                        log.warn("User {} not found", id);
                        return null;
                    } else if(cause instanceof RuntimeException) {
                        throw (RuntimeException)e.getCause();
                    } else {
                        throw new IllegalStateException(cause);
                    }
                });
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return ImmutableList.of(
                (count, token, stats) -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
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
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            private final List<Supplier<CompletableFuture<BatchResponse>>> requests = new ArrayList<>();

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    final List<AttributeType> attributes = attributes(schema, after);
                    return client.adminCreateUser(AdminCreateUserRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .userAttributes(attributes)
                            .build())
                            .thenApply(ignored -> BatchResponse.single(schema.getQualifiedName(), after));
                });
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    final List<AttributeType> attributes = attributes(schema, after);
                    return client.adminUpdateUserAttributes(AdminUpdateUserAttributesRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .userAttributes(attributes)
                            .build())
                            .thenApply(ignored -> BatchResponse.single(schema.getQualifiedName(), after));
                });
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    return client.adminDeleteUser(AdminDeleteUserRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .build())
                            .thenApply(ignored -> BatchResponse.empty());
                });
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

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
        final Long version = Instance.getVersion(after);
        if(version != null) {
            result.add(AttributeType.builder().name(CUSTOM_ATTR_PREFIX + ObjectSchema.VERSION).value(Long.toString(version)).build());
        }
        for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
            final String name = entry.getKey();
            attributes(Name.of(name), entry.getValue().getType(), after.get(name)).forEach((k, v) -> {
                final String attrName = k.toString();
                if(REQUIRED_ATTRS.contains(attrName)) {
                    result.add(AttributeType.builder().name(attrName).value(v).build());
                } else {
                    result.add(AttributeType.builder().name(CUSTOM_ATTR_PREFIX + attrName).value(v).build());
                }
            });
        }
        return result;
    }

    public Map<Name, String> attributes(final Name path, final Use<?> use, final Object value) {

        return use.visit(new Use.Visitor.Defaulting<Map<Name, String>>() {

            @Override
            public Map<Name, String> visitDefault(final Use<?> type) {

                throw new UnsupportedOperationException("Type " + type.code() + " not supported");
            }

            @Override
            public Map<Name, String> visitBoolean(final UseBoolean type) {

                return value == null ? ImmutableMap.of() : ImmutableMap.of(path, Boolean.toString(type.create(value)));
            }

            @Override
            public Map<Name, String> visitInteger(final UseInteger type) {

                return value == null ? ImmutableMap.of() : ImmutableMap.of(path, Long.toString(type.create(value)));
            }

            @Override
            public Map<Name, String> visitString(final UseString type) {

                return value == null ? ImmutableMap.of() : ImmutableMap.of(path, type.create(value));
            }

            @Override
            public Map<Name, String> visitStruct(final UseStruct type) {

                final Map<String, Object> instance = type.create(value);
                if(instance != null) {
                    final Map<Name, String> result = new HashMap<>();
                    type.getSchema().getProperties().forEach((name, prop) -> {
                        result.putAll(attributes(path.with(name), prop.getType(), instance.get(name)));
                    });
                    return result;
                } else {
                    return ImmutableMap.of();
                }
            }

            @Override
            public Map<Name, String> visitObject(final UseObject type) {

                final Instance instance = type.create(value);
                if(instance != null) {
                    return ImmutableMap.of(path.with(ObjectSchema.ID), Instance.getId(instance));
                } else {
                    return ImmutableMap.of();
                }
            }
        });
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
        Instance.setSchema(result, schema.getQualifiedName());
        Instance.setId(result, username);
        if(created != null) {
            Instance.setCreated(result, LocalDateTime.ofInstant(created, ZoneOffset.UTC));
        }
        if(updated != null) {
            Instance.setUpdated(result, LocalDateTime.ofInstant(updated, ZoneOffset.UTC));
        }
        final Map<Name, String> attrs = new HashMap<>();
        attributes.forEach(attr -> {
            final String name;
            if(attr.name().startsWith(CUSTOM_ATTR_PREFIX)) {
                name = attr.name().substring(CUSTOM_ATTR_PREFIX.length());
            } else {
                name = attr.name();
            }
            attrs.put(Name.parse(name), attr.value());
        });
        final String version = attrs.get(Name.of(ObjectSchema.VERSION));
        if(version == null) {
            Instance.setVersion(result, 1L);
        } else {
            Instance.setVersion(result, Long.valueOf(version));
        }
        schema.getProperties().forEach((name, prop) -> {
            result.put(name, from(Name.of(name), prop.getType(), attrs));
        });

        if(userStatus != null) {
            result.put("status", userStatus.toString());
        }
        result.put("enabled", enabled);
        return result;
    }

    public Object from(final Name path, final Use<?> use, final Map<Name, String> attrs) {

        return use.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public Object visitDefault(final Use<?> type) {

                throw new UnsupportedOperationException("Type " + type.code() + " not supported");
            }

            @Override
            public Object visitScalar(final UseScalar<?> type) {

                return type.create(attrs.get(path));
            }

            @Override
            public Map<String, Object> visitStruct(final UseStruct type) {

                final Map<String, Object> result = new HashMap<>();
                type.getSchema().getProperties().forEach((name, prop) -> {
                    result.put(name, from(path.with(name), prop.getType(), attrs));
                });
                if(result.isEmpty()) {
                    return null;
                } else {
                    return result;
                }
            }

            @Override
            public Map<String, Object> visitObject(final UseObject type) {

                final String id = attrs.get(path.with(ObjectSchema.ID));
                return id == null ? null : ObjectSchema.ref(id);
            }
        });
    }
}

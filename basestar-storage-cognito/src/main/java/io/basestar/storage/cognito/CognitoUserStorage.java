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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.type.Values;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.secret.Secret;
import io.basestar.storage.*;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.DisjunctionVisitor;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.util.*;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderAsyncClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class CognitoUserStorage implements DefaultLayerStorage {

    public static final int MAX_PAGE_SIZE = 60;

    private static final String CUSTOM_ATTR_PREFIX = "custom:";

    private static final Set<String> REQUIRED_ATTRS = ImmutableSet.of(
            "address", "birthdate", "email", "family_name", "gender", "given_name", "locale", "middle_name",
            "name", "nickname", "phone_number", "picture", "preferred_username", "profile", "updated_at",
            "username", "website", "zoneinfo"
    );

    private final CognitoIdentityProviderAsyncClient client;

    private final CognitoUserStrategy strategy;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

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
    public Pager<Map<String, Object>> queryObject(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return (stats, token, count) -> {
            final String userPoolId = strategy.getUserPoolId(schema);
            return client.listUsers(ListUsersRequest.builder()
                    .userPoolId(userPoolId)
                    .filter(filter(query))
                    .limit(Math.max(MAX_PAGE_SIZE, count))
                    .paginationToken(decodePaging(token))
                    .build()).thenApply(response -> {
                final List<UserType> users = response.users();
                return new Page<>(users.stream().map(v -> fromUser(schema, v))
                        .collect(Collectors.toList()), encodePaging(response.paginationToken()));
            });
        };
    }

    private String filter(final Expression query) {

        final Set<Expression> dis = query.visit(new DisjunctionVisitor());
        if(dis.size() == 1) {
            final Map<Name, Object> terms = new HashMap<>();
            final Expression sub = dis.iterator().next();
            final Map<Name, Range<Object>> ranges = sub.visit(new RangeVisitor());
            ranges.forEach((path, range) -> {
                if(range instanceof Range.Eq) {
                    final Object eq = ((Range.Eq<Object>) range).getEq();
                    terms.put(path, eq);
                }
            });
            final Object email = terms.get(Name.of("email"));
            if(email instanceof String) {
                return "email=\"" + StringEscapeUtils.escapeJava((String)email) + "\"";
            }
        }
        return null;
    }

    private String decodePaging(final Page.Token token) {

        return Nullsafe.map(token, v -> new String(v.getValue(), Charsets.UTF_8));
    }

    private Page.Token encodePaging(final String token) {

        return Nullsafe.map(token, v -> new Page.Token(token.getBytes(Charsets.UTF_8)));
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
                    futures.put(key, client.adminGetUser(AdminGetUserRequest.builder()
                            .userPoolId(userPoolId)
                            .username(key.getId())
                            .build())
                            .thenApply(v -> key.matchOrNull(fromResponse(schema, v)))
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof UserNotFoundException) {
                                    log.warn("User {} not found", key.getId());
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

                return CognitoUserStorage.this.storageTraits(schema);
            }

            @Override
            public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    final List<AttributeType> attributes = attributes(schema, after);
                    return client.adminCreateUser(AdminCreateUserRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .userAttributes(attributes)
                            .build())
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof UsernameExistsException) {
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
            public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    final List<AttributeType> attributes = attributes(schema, after);
                    return client.adminUpdateUserAttributes(AdminUpdateUserAttributesRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .userAttributes(attributes)
                            .build())
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof UserNotFoundException) {
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
            public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

                requests.add(() -> {
                    final String userPoolId = strategy.getUserPoolId(schema);
                    return client.adminDeleteUser(AdminDeleteUserRequest.builder()
                            .userPoolId(userPoolId)
                            .username(id)
                            .build())
                            .exceptionally(e -> {
                                final Throwable cause = e.getCause();
                                if(cause instanceof UserNotFoundException) {
                                    log.warn("User {} not found", id);
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

//                throw new UnsupportedOperationException("Cannot write history");
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

    private List<AttributeType> attributes(final ReferableSchema schema, final Map<String, Object> after) {

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
            public <T> Map<Name, String> visitDefault(final Use<T> type) {

                try {
                    return value == null ? ImmutableMap.of() : ImmutableMap.of(path, objectMapper.writeValueAsString(value));
                } catch (final IOException e) {
                    return ImmutableMap.of();
                }
            }

            @Override
            public <T> Map<Name, String> visitScalar(final UseScalar<T> type) {

                return value == null ? ImmutableMap.of() : ImmutableMap.of(path, Values.toString(value));
            }

            @Override
            public Map<Name, String> visitSecret(final UseSecret type) {

                if(value == null) {
                    return ImmutableMap.of();
                } else {
                    final Secret secret = type.create(value);
                    return ImmutableMap.of(path, secret.encryptedBase64());
                }
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
            public Map<Name, String> visitRef(final UseRef type) {

                final Instance instance = type.create(value);
                if(instance != null) {
                    final Long version = type.isVersioned() ? Instance.getVersion(instance) : null;
                    if(version != null) {
                        return ImmutableMap.of(
                                path.with(ObjectSchema.ID), Instance.getId(instance),
                                path.with(ObjectSchema.VERSION), Long.toString(version)
                        );
                    } else {
                        return ImmutableMap.of(path.with(ObjectSchema.ID), Instance.getId(instance));
                    }
                } else {
                    return ImmutableMap.of();
                }
            }
        });
    }

    private Map<String, Object> fromUser(final ReferableSchema schema, final UserType user) {

        return from(schema, user.username(), user.attributes(), user.enabled(), user.userStatus(),
                user.userCreateDate(), user.userLastModifiedDate());
    }

    private Map<String, Object> fromResponse(final ReferableSchema schema, final AdminGetUserResponse user) {

        return from(schema, user.username(), user.userAttributes(), user.enabled(), user.userStatus(),
                user.userCreateDate(), user.userLastModifiedDate());
    }

    private Map<String, Object> from(final ReferableSchema schema, final String username,
                                     final List<AttributeType> attributes, final boolean enabled,
                                     final UserStatusType userStatus, final Instant created, final Instant updated) {

        final Map<String, Object> result = new HashMap<>();
        Instance.setSchema(result, schema.getQualifiedName());
        Instance.setId(result, username);
        if(created != null) {
            Instance.setCreated(result, created);
        }
        if(updated != null) {
            Instance.setUpdated(result, updated);
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
            public <T> Object visitDefault(final Use<T> type) {

                try {
                    final String value = attrs.get(path);
                    return value == null ? null : objectMapper.readValue(value, Object.class);
                } catch (final IOException e) {
                    return ImmutableMap.of();
                }
            }

            @Override
            public <T> Object visitScalar(final UseScalar<T> type) {

                return type.create(attrs.get(path));
            }

            @Override
            public Secret visitSecret(final UseSecret type) {

                final String value = attrs.get(path);
                if(value == null) {
                    return null;
                } else {
                    return Secret.encrypted(value);
                }
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
            public Map<String, Object> visitRef(final UseRef type) {

                final String id = attrs.get(path.with(ObjectSchema.ID));
                if(id == null) {
                    return null;
                } else {
                    if (type.isVersioned()) {
                        final String versionStr = attrs.get(path.with(ObjectSchema.VERSION));
                        final Long version = versionStr == null ? null : Long.parseLong(versionStr);
                        return version == null ? ReferableSchema.ref(id) : ReferableSchema.versionedRef(id, version);
                    } else {
                        return ReferableSchema.ref(id);
                    }
                }
            }
        });
    }
}

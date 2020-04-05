package io.basestar.storage.cognito;

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
                .thenApply(this::fromResponse);
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
                        return new PagedList<>(users.stream().map(this::fromUser)
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

            private List<Supplier<CompletableFuture<BatchResponse>>> requests = new ArrayList<>();

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
        for(final Map.Entry<String, Property> entry : schema.getAllProperties().entrySet()) {
            final String name = entry.getKey();
            final String value = Objects.toString(after.get(name));
            result.add(AttributeType.builder().name(name).value(value).build());
        }
        return result;
    }

    private Map<String, Object> fromUser(final UserType user) {

        return from(user.username(), user.attributes(), user.enabled(), user.userStatus(),
                user.userCreateDate(), user.userLastModifiedDate());
    }

    private Map<String, Object> fromResponse(final AdminGetUserResponse user) {

        return from(user.username(), user.userAttributes(), user.enabled(), user.userStatus(),
                user.userCreateDate(), user.userLastModifiedDate());
    }

    private Map<String, Object> from(final String username, final List<AttributeType> attributes,
                                     final boolean enabled, final UserStatusType userStatus,
                                     final Instant created, final Instant updated) {

        final Map<String, Object> result = new HashMap<>();
        Instance.setId(result, username);
        attributes.forEach(attr -> result.put(attr.name(), attr.value()));
        result.put("status", userStatus.toString());
        result.put("enabled", enabled);
        return result;
    }
}

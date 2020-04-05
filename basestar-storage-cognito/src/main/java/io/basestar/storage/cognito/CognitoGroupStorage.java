package io.basestar.storage.cognito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CognitoGroupStorage implements Storage {

    private static final String DESCRIPTION_KEY = "description";

    private final CognitoIdentityProviderAsyncClient client;

    private final CognitoGroupRouting routing;

    private CognitoGroupStorage(final Builder builder) {

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

        private CognitoGroupRouting routing;

        public CognitoGroupStorage build() {

            return new CognitoGroupStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final String userPoolId = routing.getUserPoolId(schema);
        return client.getGroup(GetGroupRequest.builder()
                .userPoolId(userPoolId)
                .groupName(id)
                .build())
                .thenApply(response -> fromGroup(response.group()));
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
                    return client.listGroups(ListGroupsRequest.builder()
                            .userPoolId(userPoolId)
                            .limit(count)
                            .nextToken(decodePaging(token))
                            .build()).thenApply(response -> {
                        final List<GroupType> groups = response.groups();
                        return new PagedList<>(groups.stream().map(this::fromGroup)
                                .collect(Collectors.toList()), encodePaging(response.nextToken()));

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
                    final String description = null;
                    return client.createGroup(CreateGroupRequest.builder()
                            .userPoolId(userPoolId)
                            .groupName(id)
                            .description(description)
                            .build())
                            .thenApply(ignored -> BatchResponse.single(schema.getName(), after));
                });
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                requests.add(() -> {
                    final String userPoolId = routing.getUserPoolId(schema);
                    final String description = null;
                    return client.updateGroup(UpdateGroupRequest.builder()
                            .userPoolId(userPoolId)
                            .groupName(id)
                            .description(description)
                            .build())
                            .thenApply(ignored -> BatchResponse.single(schema.getName(), after));
                });
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                requests.add(() -> {
                    final String userPoolId = routing.getUserPoolId(schema);
                    return client.deleteGroup(DeleteGroupRequest.builder()
                            .userPoolId(userPoolId)
                            .groupName(id)
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

    public String getDescription(final Map<String, Object> data) {

        return (String)data.get(DESCRIPTION_KEY);
    }

    private Map<String, Object> fromGroup(final GroupType group) {

        return ImmutableMap.of(
                Reserved.ID, group.groupName(),
                DESCRIPTION_KEY, group.description()
        );
    }
}

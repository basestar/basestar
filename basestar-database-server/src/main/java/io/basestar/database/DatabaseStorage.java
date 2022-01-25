package io.basestar.database;

import io.basestar.auth.Caller;
import io.basestar.database.options.*;
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.util.*;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@RequiredArgsConstructor
public class DatabaseStorage implements Storage {

    private final DatabaseServer database;

    private final Caller caller;

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            final BatchCapture capture = new BatchCapture();

            @Override
            public ReadTransaction get(final ReferableSchema schema, final String id, final Set<Name> expand) {

                capture.captureLatest(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

                capture.captureVersion(schema, id, version, expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                final Map<BatchResponse.RefKey, CompletableFuture<Map<String, Object>>> futures = new HashMap<>();
                capture.forEachRef((schema, key, args) -> {
                    futures.put(key, database.read(caller, ReadOptions.builder()
                            .setSchema(schema.getQualifiedName())
                            .setId(key.getId())
                            .setVersion(key.getVersion())
                            .setExpand(args.getExpand())
                            .build()).thenApply(v -> v));
                });
                return CompletableFutures.allOf(futures).thenApply(BatchResponse::new);
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            private final List<ActionOptions> actions = new ArrayList<>();

            @Override
            public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

                throw new UnsupportedOperationException();
            }

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                actions.add(CreateOptions.builder()
                        .setSchema(schema.getQualifiedName())
                        .setId(id)
                        .setData(after)
                        .build());
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                actions.add(UpdateOptions.builder()
                        .setSchema(schema.getQualifiedName())
                        .setId(id)
                        .setData(after)
                        .setVersion(before == null ? null : Instance.getVersion(before))
                        .setMode(UpdateOptions.Mode.REPLACE)
                        .build());
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                actions.add(DeleteOptions.builder()
                        .setSchema(schema.getQualifiedName())
                        .setId(id)
                        .setVersion(before == null ? null : Instance.getVersion(before))
                        .build());
                return this;
            }

            @Override
            public WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                // no-op because the database already handles history
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                final BatchOptions.Builder batch = BatchOptions.builder();
                for (int i = 0; i != actions.size(); ++i) {
                    batch.putAction("action" + i, actions.get(i));
                }
                return database.batch(caller, batch.build()).thenApply(response -> {
                    final Map<BatchResponse.RefKey, Map<String, Object>> results = new HashMap<>();
                    for (int i = 0; i != actions.size(); ++i) {
                        final ActionOptions action = actions.get(i);
                        final Instance result = response.get("action" + i);
                        if (result != null) {
                            results.put(BatchResponse.RefKey.latest(action.getSchema(), action.getId()), result);
                        }
                    }
                    return BatchResponse.fromRefs(results);
                }).exceptionally(t -> {
                    try {
                        throw t.getCause();
                    } catch (final ObjectMissingException e) {
                        throw new VersionMismatchException(e.getSchema(), e.getId(), null);
                    } catch (final RuntimeException e) {
                        throw e;
                    } catch (final Throwable e) {
                        throw new CompletionException(e);
                    }
                });
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return EventStrategy.SUPPRESS;
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        // FIXME: expose properly
        return database.storage.storageTraits(schema);
    }

    @Override
    public Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return database.storage.supportedExpand(schema, expand);
    }

    @Override
    public void validate(final ObjectSchema schema) {

    }

    @Override
    public Pager<Map<String, Object>> query(final Consistency consistency, final QueryableSchema schema, final Map<String, Object> arguments, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return (stats, paging, count) -> database.query(caller, QueryOptions.builder()
                .setConsistency(consistency)
                .setSchema(schema.getQualifiedName())
                .setArguments(arguments)
                .setExpression(query)
                .setSort(sort)
                .setExpand(expand)
                .setStats(stats)
                .setCount(count)
                .setPaging(paging)
                .build()).thenApply(page -> page.map(instance -> instance));
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return (stats, paging, count) -> database.queryHistory(caller, QueryHistoryOptions.builder()
                .setConsistency(consistency)
                .setSchema(schema.getQualifiedName())
                .setId(id)
                .setExpression(query)
                .setSort(sort)
                .setExpand(expand)
                .setStats(stats)
                .setCount(count)
                .setPaging(paging)
                .build()).thenApply(page -> page.map(instance -> instance));
    }

    @Override
    public CompletableFuture<Set<Event>> afterCreate(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        return CompletableFuture.completedFuture(Immutable.set());
//        return database.storage.afterCreate(schema, id, after);
    }

    @Override
    public CompletableFuture<Set<Event>> afterUpdate(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {

        return CompletableFuture.completedFuture(Immutable.set());
//        return database.storage.afterUpdate(schema, id, version, before, after);
    }

    @Override
    public CompletableFuture<Set<Event>> afterDelete(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {

        return CompletableFuture.completedFuture(Immutable.set());
//        return database.storage.afterDelete(schema, id, version, before);
    }
}

package io.basestar.storage;

import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.util.Pager;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.PagedList;
import io.basestar.util.Sort;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ConstantStorage implements Storage {

    private final Map<Name, Map<String, Map<String, Object>>> data;

    @lombok.Builder(builderClassName = "Builder")
    ConstantStorage(final Collection<? extends Map<String, Object>> items) {

        final Map<Name, Map<String, Map<String, Object>>> data = new HashMap<>();
        items.forEach(item -> {
            final Name schema = Instance.getSchema(item);
            final String id = Instance.getId(item);
            final Map<String, Map<String, Object>> target = data.computeIfAbsent(schema, ignored -> new HashMap<>());
            target.put(id, item);
        });
        this.data = Collections.unmodifiableMap(data);
    }

    private Map<String, Object> get(final Name schema, final String id) {

        return Nullsafe.option(data.get(schema)).get(id);
    }

    private Map<String, Object> get(final Map<String, Object> instance) {

        return get(Instance.getSchema(instance), Instance.getId(instance));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final Map<String, Object> object = get(schema.getQualifiedName(), id);
        return CompletableFuture.completedFuture(object);
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        final Map<String, Object> object = get(schema.getQualifiedName(), id);
        if(object != null && Long.valueOf(version).equals(Instance.getVersion(object))) {
            return CompletableFuture.completedFuture(object);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        // Add a source that will emit matching results

        return ImmutableList.of((count, token, stats) -> {
            // Don't do any paging, just return all matches, this is compliant with Pager interface
            final List<Map<String, Object>> page = new ArrayList<>();
            Nullsafe.option(data.get(schema.getQualifiedName())).forEach((id, item) -> {
                if(query.evaluatePredicate(Context.init(item))) {
                    page.add(item);
                }
            });

            // Must be sorted
            final Comparator<Map<String, Object>> comparator = Sort.comparator(sort, (t, path) -> (Comparable)path.apply(t));
            page.sort(comparator);

            return CompletableFuture.completedFuture(new PagedList<>(page, null));
        });
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        throw new UnsupportedOperationException();
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return new StorageTraits() {
            @Override
            public Consistency getHistoryConsistency() {

                return Consistency.ATOMIC;
            }

            @Override
            public Consistency getSingleValueIndexConsistency() {

                return Consistency.ATOMIC;
            }

            @Override
            public Consistency getMultiValueIndexConsistency() {

                return Consistency.ATOMIC;
            }

            @Override
            public boolean supportsPolymorphism() {

                return true;
            }

            @Override
            public boolean supportsMultiObject() {

                return true;
            }

            @Override
            public Concurrency getObjectConcurrency() {

                return Concurrency.OPTIMISTIC;
            }
        };
    }
}

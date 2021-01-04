package io.basestar.storage;

import io.basestar.schema.ReferableSchema;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Data
public class BatchCapture {

    private final Map<ReferableSchema, Map<BatchResponse.RefKey, RefArgs>> refs;

    public BatchCapture() {

        this.refs = new HashMap<>();
    }

    public BatchCapture(final Map<ReferableSchema, Map<BatchResponse.RefKey, RefArgs>> refs) {//}, final Map<LinkableSchema, Map<BatchResponse.QueryKey, QueryArgs>> queries) {

        this.refs = new HashMap<>(refs);
    }

    public interface ForEachRefConsumer {

        void accept(ReferableSchema schema, BatchResponse.RefKey key, RefArgs args);
    }

    public void forEachRef(final ForEachRefConsumer consumer) {

        refs.forEach((schema, schemaRefs) ->
                schemaRefs.forEach((key, args) ->
                        consumer.accept(schema, key, args)));
    }

    public interface MapRefsFunction<T> {

        T apply(ReferableSchema schema, BatchResponse.RefKey key, RefArgs args);
    }

    public <T> Map<BatchResponse.RefKey, T> mapRefs(final MapRefsFunction<T> fn) {

        final Map<BatchResponse.RefKey, T> result = new HashMap<>();
        forEachRef(((schema, key, args) -> result.put(key, fn.apply(schema, key, args))));
        return result;
    }

    @Data
    public static class RefArgs {

        private final Set<Name> expand;

        public static RefArgs merge(@Nullable final RefArgs a, final Set<Name> expand) {

            if(a == null) {
                return new RefArgs(expand);
            } else {
                return new RefArgs(Immutable.addAll(a.getExpand(), expand));
            }
        }
    }

    public void captureLatest(final ReferableSchema schema, final String id, final Set<Name> expand) {

        refs.computeIfAbsent(schema, ignored -> new HashMap<>())
                .compute(BatchResponse.RefKey.latest(schema.getQualifiedName(), id),
                        (k, v) -> RefArgs.merge(v, expand));
    }

    public void captureVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

        refs.computeIfAbsent(schema, ignored -> new HashMap<>())
                .compute(BatchResponse.RefKey.version(schema.getQualifiedName(), id, version),
                        (k, v) -> RefArgs.merge(v, expand));
    }
}

package io.basestar.spark.combiner;


import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Instance;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBoolean;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.util.ISO8601;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.Tuple2;

import java.time.Instant;
import java.util.*;

public interface Combiner extends Serializable {

    String DEFAULT_JOIN_TYPE = "full_outer";

    Simple SIMPLE = new Simple();

    Delta DELTA = new Delta();

    Optional<Map<String, Object>> apply(LinkableSchema schema, Map<String, Object> before, Map<String, Object> after);

    default Dataset<Row> apply(final LinkableSchema schema, final Set<Name> expand, final Dataset<Row> baseline, final Dataset<Row> overlay) {

        return apply(schema, expand, baseline, overlay, DEFAULT_JOIN_TYPE);
    }

    default Dataset<Row> apply(final LinkableSchema schema, final Set<Name> expand, final Dataset<Row> baseline, final Dataset<Row> overlay, final String joinType) {

        final Column condition = baseline.col(schema.id()).equalTo(overlay.col(schema.id()));
        final Dataset<Tuple2<Row, Row>> joined = baseline.joinWith(overlay, condition, joinType);
        return apply(schema, expand, joined);
    }

    default Dataset<Row> apply(final LinkableSchema schema, final Dataset<Tuple2<Row, Row>> joined) {

        return apply(schema, schema.getExpand(), joined);
    }

    default Dataset<Row> apply(final LinkableSchema schema, final Set<Name> expand, final Dataset<Tuple2<Row, Row>> joined) {

        final Map<String, Use<?>> extraMetadata = ImmutableMap.of(
                Reserved.DELETED, UseBoolean.DEFAULT
        );

        final StructType structType = SparkSchemaUtils.structType(schema, expand, extraMetadata);
        return joined.flatMap(SparkUtils.flatMap(pair -> {

            final Map<String, Object> before = SparkSchemaUtils.fromSpark(schema, expand, SparkRowUtils.nulled(pair._1()));
            final Map<String, Object> after = SparkSchemaUtils.fromSpark(schema, expand, SparkRowUtils.nulled(pair._2()));

            final Optional<Map<String, Object>> result = apply(schema, before, after);
            if(result.isPresent()) {
                final Row row = SparkSchemaUtils.toSpark(schema, expand, extraMetadata, structType, result.get());
                return Collections.singleton(row).iterator();
            } else {
                return Collections.emptyIterator();
            }

        }), RowEncoder.apply(structType));
    }

    class Simple implements Combiner {

        @Override
        public Optional<Map<String, Object>> apply(final LinkableSchema schema, final Map<String, Object> before, final Map<String, Object> after) {

            if(after == null) {
                return Optional.of(before);
            } else {
                return Optional.of(after);
            }
        }
    }

    class Consistent implements Combiner {

        private final Instant timestamp;

        private final Conflict conflict;

        public enum Conflict {

            ERROR,
            REPAIR
        }

        @lombok.Builder(builderClassName = "Builder")
        public Consistent(final Instant timestamp, final Conflict conflict) {

            this.timestamp = Nullsafe.orDefault(timestamp, ISO8601::now);
            this.conflict = Nullsafe.orDefault(conflict, Conflict.REPAIR);
        }

        @Override
        public Optional<Map<String, Object>> apply(final LinkableSchema schema, final Map<String, Object> before, final Map<String, Object> after) {

            if(after == null) {
                return Optional.of(before);
            } else {
                if (schema instanceof ObjectSchema) {
                    final long beforeVersion;
                    final Instant created;
                    final Instant updated = Nullsafe.orDefault(Instance.getUpdated(after), timestamp);
                    if (before == null) {
                        beforeVersion = 0L;
                        created = updated;
                    } else {
                        beforeVersion = Nullsafe.orDefault(Instance.getVersion(before), 0L);
                        // Must be left as null if already null, because created might be part of the bucketing scheme
                        created = Instance.getCreated(before);
                    }
                    final long version = beforeVersion + 1L;
                    if (conflict == Conflict.ERROR) {
                        final long afterVersion = MoreObjects.firstNonNull(Instance.getVersion(after), 0L);
                        if (afterVersion != version) {
                            throw new IllegalStateException("Object of type " + schema.getQualifiedName() + " with id " + Instance.getId(after)
                                    + " expected to be at version " + version + " but at version " + afterVersion);
                        }
                    }
                    final Map<String, Object> result = new HashMap<>(after);
                    result.put(ObjectSchema.VERSION, version);
                    result.put(ObjectSchema.CREATED, created);
                    result.put(ObjectSchema.UPDATED, updated);
                    result.put(ObjectSchema.HASH, schema.hash(result));
                    return Optional.of(result);
                } else {
                    // View schema
                    return Optional.of(after);
                }
            }
        }
    }

    class Delta implements Combiner {

        @Override
        public Optional<Map<String, Object>> apply(final LinkableSchema schema, final Map<String, Object> before, final Map<String, Object> after) {

            if(schema.areEqual(before, after)) {
                return Optional.empty();
            } else if(after == null) {
                return Optional.of(schema.deleted(Instance.getId(before)));
            } else {
                return Optional.of(Immutable.put(after, Reserved.DELETED, false));
            }
        }
    }
}

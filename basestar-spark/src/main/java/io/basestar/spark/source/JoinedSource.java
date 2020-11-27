package io.basestar.spark.source;

import com.google.common.collect.ImmutableList;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.sink.Sink;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

import java.util.List;

public class JoinedSource<T> implements Source<Dataset<Tuple2<T, T>>> {

    public static final String DEFAULT_JOIN_TYPE = "full_outer";

    private final Source<Dataset<T>> left;

    private final Source<Dataset<T>> right;

    private final List<String> idColumns;

    private final String joinType;

    @lombok.Builder(builderClassName = "Builder")
    JoinedSource(final Source<Dataset<T>> left, final Source<Dataset<T>> right, final List<String> idColumns, final String joinType) {

        this.left = Nullsafe.require(left);
        this.right = Nullsafe.require(right);
        this.idColumns = Nullsafe.orDefault(idColumns, ImmutableList.of(ObjectSchema.ID));
        this.joinType = Nullsafe.orDefault(joinType, DEFAULT_JOIN_TYPE);
    }

    @Override
    public void then(final Sink<Dataset<Tuple2<T, T>>> sink) {

        left.then(l -> {
            right.then(r -> {
                final Column condition = idColumns.stream().map(c -> l.col(c).equalTo(r.col(c))).reduce(Column::and)
                        .orElseThrow(IllegalStateException::new);
                sink.accept(l.joinWith(r, condition, joinType));
            });
        });
    }
}

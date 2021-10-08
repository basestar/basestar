package io.basestar.spark.query;

import com.google.common.collect.ImmutableList;
import io.basestar.mapper.MappingContext;
import io.basestar.spark.source.Source;
import io.basestar.spark.transform.MarshallTransform;
import io.basestar.spark.transform.SortTransform;
import io.basestar.spark.transform.Transform;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public interface Query<T> {

    Dataset<T> dataset();

    default List<T> collectAsList() {

        return dataset().collectAsList();
    }

    default <T2> Query<T2> then(final Transform<Dataset<T>, Dataset<T2>> transform) {

        return new TransformChaining<>(this, transform);
    }

    default Query<Row> select(final Column... columns) {

        return then(ds -> ds.select(columns));
    }

    default Query<T> sort(final Sort... sort) {

        return sort(ImmutableList.copyOf(sort));
    }

    default Query<T> sort(final List<Sort> sort) {

        return then(SortTransform.<T>builder().sort(sort).build());
    }

    default <T2> Query<T2> as(final Class<T2> marshallAs) {

        return as(new MappingContext(), marshallAs);
    }

    default <T2> Query<T2> as(final MappingContext context, final Class<T2> marshallAs) {

        final Query<T> self = this;
        final MarshallTransform<T2> marshall = MarshallTransform.<T2>builder().context(context).targetType(marshallAs).build();
        return () -> marshall.accept(self.dataset().toDF());
    }

    default Source<Dataset<T>> source() {

        return sink -> sink.accept(dataset());
    }

    /**
     * Chaining is slightly more complex to support FoldingTransform
     */

    @RequiredArgsConstructor
    class TransformChaining<T, T2> implements Query<T2> {

        private final Query<T> delegate;

        private final Transform<Dataset<T>, Dataset<T2>> chain;

        @Override
        public Dataset<T2> dataset() {

            return chain.accept(delegate.dataset());
        }

        @Override
        public <T3> Query<T3> then(final Transform<Dataset<T2>, Dataset<T3>> transform) {

            return new TransformChaining<>(delegate, chain.then(transform));
        }
    }
}

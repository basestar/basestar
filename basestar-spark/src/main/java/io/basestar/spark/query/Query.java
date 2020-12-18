package io.basestar.spark.query;

import io.basestar.spark.source.Source;
import io.basestar.spark.transform.Transform;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Query {

    Dataset<Row> result();

    default Query then(final Transform<Dataset<Row>, Dataset<Row>> transform) {

        return new TransformChaining(this, transform);
    }

    default Source<Dataset<Row>> source() {

        return sink -> sink.accept(result());
    }

    /**
     * Chaining is slightly more complex to support FoldingTransform
     */

    @RequiredArgsConstructor
    class TransformChaining implements Query {

        private final Query delegate;

        private final Transform<Dataset<Row>, Dataset<Row>> chain;

        @Override
        public Dataset<Row> result() {

            return chain.accept(delegate.result());
        }

        @Override
        public Query then(final Transform<Dataset<Row>, Dataset<Row>> transform) {

            return new TransformChaining(delegate, chain.then(transform));
        }
    }
}

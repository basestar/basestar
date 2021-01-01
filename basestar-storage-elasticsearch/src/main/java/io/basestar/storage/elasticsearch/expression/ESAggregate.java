package io.basestar.storage.elasticsearch.expression;

import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.util.Immutable;
import lombok.Data;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

public interface ESAggregate {

    List<AggregationBuilder> builders();

    static AggregationBuilder applyInput(final ValuesSourceAggregationBuilder.LeafOnly<?, ?> builder, final Expression input) {

        if(input instanceof NameConstant) {
            final String field = ((NameConstant) input).getName().toString();
            return builder.field(field);
        } else {
            throw new UnsupportedOperationException("Aggregation with non-field input not supported (yet)");
        }
    }

    interface Bucket {

        long getDocCount();

        Aggregations getAggregations();
    }

    Object read(Bucket bucket);

    default Object read(final CompositeAggregation.Bucket bucket) {

        return read(new Bucket() {
            @Override
            public long getDocCount() {

                return bucket.getDocCount();
            }

            @Override
            public Aggregations getAggregations() {

                return bucket.getAggregations();
            }
        });
    }

    @Data
    class Count implements ESAggregate {

        @Override
        public List<AggregationBuilder> builders() {

            // Count metric is always calculated anyway
            return Immutable.list();
        }

        @Override
        public Object read(final Bucket bucket) {

            return bucket.getDocCount();
        }
    }

    @Data
    class CountIf implements ESAggregate {

        private final Expression predicate;

        private String name() {

            return "count_" + predicate.digest();
        }

        @Override
        public List<AggregationBuilder> builders() {

            final QueryBuilder query = new ESExpressionVisitor().visit(predicate);
            return Immutable.list(AggregationBuilders.filter(name(), query));
        }

        @Override
        public Object read(final Bucket bucket) {

            final SingleBucketAggregation agg = bucket.getAggregations().get(name());
            return agg.getDocCount();
        }
    }

    @Data
    class Avg implements ESAggregate {

        private final Expression input;

        private String name() {

            return "avg_" + input.digest();
        }

        @Override
        public List<AggregationBuilder> builders() {

            return Immutable.list(applyInput(AggregationBuilders.avg(name()), input));
        }

        @Override
        public Object read(final Bucket bucket) {

            final Aggregation agg = bucket.getAggregations().get(name());
            return null;
        }
    }

    @Data
    class NumberMin implements ESAggregate {

        private final Expression input;

        private String name() {

            return "min_" + input.digest();
        }

        @Override
        public List<AggregationBuilder> builders() {

            return Immutable.list(applyInput(AggregationBuilders.max(name()), input));
        }

        @Override
        public Object read(final Bucket bucket) {

            final Aggregation agg = bucket.getAggregations().get(name());
            return null;
        }
    }

    @Data
    class NumberMax implements ESAggregate {

        private final Expression input;

        private String name() {

            return "max_" + input.digest();
        }

        @Override
        public List<AggregationBuilder> builders() {

            return Immutable.list(applyInput(AggregationBuilders.max(name()), input));
        }

        @Override
        public Object read(final Bucket bucket) {

            final Aggregation agg = bucket.getAggregations().get(name());
            return null;
        }
    }

    interface StringMinMax extends ESAggregate {

        String name();

        SortOrder order();

        Expression getInput();

        @Override
        default List<AggregationBuilder> builders() {

            final Expression input = getInput();
            if(input instanceof NameConstant) {
                final String field = ((NameConstant) input).getName().toString();
                final AggregationBuilder agg = AggregationBuilders.topHits(name())
                        .docValueField(field).sort(field, order())
                        .fetchSource(false)
                        .size(1);
                return Immutable.list(agg);
            } else {
                throw new UnsupportedOperationException("Aggregation with non-field input not supported (yet)");
            }
        }

        @Override
        default Object read(final Bucket bucket) {

            final Expression input = getInput();
            if(input instanceof NameConstant) {
                final String field = ((NameConstant) input).getName().toString();
                final TopHits agg = bucket.getAggregations().get(name());
                final SearchHit[] hits = agg.getHits().getHits();
                if(hits.length > 0) {
                    final DocumentField docField = hits[0].field(field);
                    return docField == null ? null : docField.getValue();
                } else {
                    return null;
                }
            } else {
                throw new UnsupportedOperationException("Aggregation with non-field input not supported (yet)");
            }
        }
    }

    @Data
    class StringMin implements StringMinMax {

        private final Expression input;

        @Override
        public String name() {

            return "min_" + input.digest();
        }

        @Override
        public SortOrder order() {

            return SortOrder.ASC;
        }
    }

    @Data
    class StringMax implements StringMinMax {

        private final Expression input;

        @Override
        public String name() {

            return "max_" + input.digest();
        }

        @Override
        public SortOrder order() {

            return SortOrder.DESC;
        }
    }

    @Data
    class Sum implements ESAggregate {

        private final Expression input;

        private String name() {

            return "sum_" + input.digest();
        }

        @Override
        public List<AggregationBuilder> builders() {

            return Immutable.list(applyInput(AggregationBuilders.sum(name()), input));
        }

        @Override
        public Object read(final Bucket bucket) {

            final Aggregation agg = bucket.getAggregations().get(name());
            return null;
        }
    }
}

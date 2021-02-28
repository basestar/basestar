package io.basestar.storage.elasticsearch.query;

import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.logical.And;
import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.storage.elasticsearch.ElasticsearchStrategy;
import io.basestar.storage.elasticsearch.ElasticsearchUtils;
import io.basestar.storage.elasticsearch.expression.ESAggregate;
import io.basestar.storage.elasticsearch.expression.ESAggregateVisitor;
import io.basestar.storage.elasticsearch.expression.ESExpressionVisitor;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.util.KeysetPagingUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

public interface ESQueryStage {

    ESQueryStage filter(Expression filter);

    ESQueryStage sort(List<Sort> sort);

    ESQueryStage agg(List<String> group, Map<String, TypedExpression<?>> expressions);

    ESQueryStage map(Map<String, TypedExpression<?>> expressions);

    Layout layout();

    Optional<SearchRequest> request(Set<Page.Stat> stats, Page.Token token, int count);

    Page<Map<String, Object>> page(Mappings mappings, Set<Page.Stat> stats, Page.Token token, int count, SearchResponse response);

    @Data
    @Slf4j
    @RequiredArgsConstructor
    class Source implements ESQueryStage {

        private final ElasticsearchStrategy strategy;

        private final LinkableSchema schema;

        private final Expression filter;

        private final List<Sort> sort;

        public Source(final ElasticsearchStrategy strategy, final LinkableSchema schema) {

            this(strategy, schema, null, Immutable.list());
        }

        @Override
        public ESQueryStage filter(final Expression filter) {

            final Expression newFilter;
            if(this.filter != null) {
                newFilter = new And(this.filter, filter);
            } else {
                newFilter = filter;
            }
            return new Source(strategy, schema, newFilter, sort);
        }

        @Override
        public ESQueryStage agg(final List<String> group, Map<String, TypedExpression<?>> expressions) {

            return new Agg(this, group, null, expressions);
        }

        @Override
        public ESQueryStage map(final Map<String, TypedExpression<?>> expressions) {

            throw new UnsupportedOperationException();
        }

        @Override
        public ESQueryStage sort(final List<Sort> sort) {

            return new Source(strategy, schema, filter, sort);
        }

        @Override
        public Layout layout() {

            return schema;
        }

        @Override
        public Optional<SearchRequest> request(final Set<Page.Stat> stats, final Page.Token token, final int count) {

            final SearchRequest request = new SearchRequest(strategy.index(schema));
            final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            final QueryBuilder basicQuery;
            if(filter != null) {
                basicQuery = new ESExpressionVisitor().visit(filter);
            } else {
                basicQuery = null;
            }
            final QueryBuilder query;
            if(count != 0) {
                final List<Sort> normalizedSort = KeysetPagingUtils.normalizeSort(schema, sort);
                normalizedSort.forEach(s -> sourceBuilder.sort(ElasticsearchUtils.sort(s)));
                if (token == null) {
                    query = basicQuery;
                } else if (basicQuery == null) {
                    query = ElasticsearchUtils.pagingQueryBuilder(schema, normalizedSort, token);
                } else {
                    query = QueryBuilders.boolQuery()
                            .must(basicQuery)
                            .must(ElasticsearchUtils.pagingQueryBuilder(schema, normalizedSort, token));
                }
            } else {
                query = basicQuery;
            }
            sourceBuilder.query(query);
            sourceBuilder.size(count);
            sourceBuilder.trackTotalHits(true);
            request.source(sourceBuilder);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            return Optional.of(request);
        }

        @Override
        public Page<Map<String, Object>> page(final Mappings mappings, final Set<Page.Stat> stats, final Page.Token token, final int count, final SearchResponse response) {

            final List<Sort> normalizedSort = KeysetPagingUtils.normalizeSort(schema, sort);
            final SearchHits hits = response.getHits();

            final List<Map<String, Object>> results = new ArrayList<>();
            Map<String, Object> last = null;
            for (final SearchHit hit : hits) {
                last = mappings.fromSource(hit.getSourceAsMap());
                results.add(last);
            }
            final long total = hits.getTotalHits().value;
            final Page.Token newPaging;
            if (total > results.size() && last != null) {
                newPaging = KeysetPagingUtils.keysetPagingToken(schema, normalizedSort, last);
            } else {
                newPaging = null;
            }
            return new Page<>(results, newPaging, Page.Stats.fromTotal(total));
        }
    }

    @Data
    @Slf4j
    class Agg implements ESQueryStage {

        private final ESQueryStage source;

        private final List<String> group;

        private final List<Sort> sort;

        private final Map<String, TypedExpression<?>> expressions;

        @Override
        public ESQueryStage filter(final Expression condition) {

            throw new UnsupportedOperationException("Post-aggregate filtering is not supported (yet)");
        }

        @Override
        public ESQueryStage agg(final List<String> group, final Map<String, TypedExpression<?>> aggregates) {

            throw new UnsupportedOperationException("Aggregating aggregates is not supported");
        }

        @Override
        public ESQueryStage map(final Map<String, TypedExpression<?>> expressions) {

            throw new UnsupportedOperationException();
        }

        @Override
        public ESQueryStage sort(final List<Sort> sort) {

            if(sort.stream().allMatch(s -> group.contains(s.getName().toString()))) {
                return new Agg(source, group, sort, expressions);
            } else {
                throw new UnsupportedOperationException("Sorting of aggregates by non-group terms is not supported");
            }
        }

        @Override
        public Layout layout() {

            return Layout.simple(Immutable.transformValues(expressions, (k, v) -> v.getType()));
        }

        private CompositeValuesSourceBuilder<?> groupSource(final String name) {

            final TypedExpression<?> expr = expressions.get(name);
            if(expr.getExpression() instanceof NameConstant) {
                final Name field = ((NameConstant) expr.getExpression()).getName();
                return new TermsValuesSourceBuilder(name).field(field.toString())
                        .missingBucket(true).order(order(name));
            } else {
                throw new UnsupportedOperationException("Non-property grouping not supported (yet)");
            }
        }

        private SortOrder order(final String name) {

            if(sort == null) {
                return SortOrder.ASC;
            } else {
                return sort.stream().filter(v -> v.getName().toString().equals(name))
                        .map(v -> ElasticsearchUtils.order(v.getOrder())).findFirst().orElse(SortOrder.ASC);
            }
        }

        private Map<String, ESAggregate> aggs() {

            final InferenceContext inference = InferenceContext.from(source.layout());
            final ESAggregateVisitor aggVisitor = new ESAggregateVisitor(inference);
            final Map<String, ESAggregate> aggs = new HashMap<>();
            expressions.forEach((k, expr) -> {
                if(!group.contains(k)) {
                    aggs.put(k, aggVisitor.visit(expr.getExpression()));
                }
            });
            return aggs;
        }

        private List<AggregationBuilder> subAggs() {

            final List<AggregationBuilder> aggs = new ArrayList<>();
            final Set<String> seen = new HashSet<>();
            aggs().values().forEach(agg -> agg.builders().forEach(builder -> {
                if(!seen.contains(builder.getName())) {
                    seen.add(builder.getName());
                    aggs.add(builder);
                }
            }));
            return aggs;
        }

        private List<AggregationBuilder> aggs(final Set<Page.Stat> stats, final Page.Token token, final int count) {

            if(!group.isEmpty()) {
                final List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
                group.forEach(g -> sources.add(groupSource(g)));
                final CompositeAggregationBuilder agg = AggregationBuilders.composite("group", sources);
                agg.size(count);
                subAggs().forEach(agg::subAggregation);
                return Immutable.list(agg);
            } else {
                return subAggs();
            }
        }

        @Override
        public Optional<SearchRequest> request(final Set<Page.Stat> stats, final Page.Token token, final int count) {

            return source.request(Immutable.set(), null, 0).map(request -> {
                final SearchSourceBuilder sourceBuilder = request.source();
                aggs(stats, token, count).forEach(sourceBuilder::aggregation);
                return request;
            });
        }

        @Override
        public Page<Map<String, Object>> page(final Mappings mappings, final Set<Page.Stat> stats, final Page.Token token, final int count, final SearchResponse response) {

            if(!group.isEmpty()) {
                final List<Map<String, Object>> results = new ArrayList<>();
                final CompositeAggregation group = response.getAggregations().get("group");
                final Map<String, ESAggregate> aggs = aggs();
                for(final CompositeAggregation.Bucket bucket : group.getBuckets()) {
                    final Map<String, Object> result = new HashMap<>(bucket.getKey());
                    aggs.forEach((key, agg) -> result.put(key, agg.read(bucket)));
                    results.add(mappings.fromSource(result));
                }
                return Page.from(results);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Data
    class Empty implements ESQueryStage {

        private final Layout layout;

        @Override
        public ESQueryStage filter(final Expression condition) {

            return this;
        }

        @Override
        public ESQueryStage agg(final List<String> group, final Map<String, TypedExpression<?>> aggregates) {

            return this;
        }

        @Override
        public ESQueryStage map(final Map<String, TypedExpression<?>> expressions) {

            return this;
        }

        @Override
        public ESQueryStage sort(final List<Sort> sort) {

            return this;
        }

        @Override
        public Layout layout() {

            return layout;
        }

        @Override
        public Optional<SearchRequest> request(final Set<Page.Stat> stats, final Page.Token token, final int count) {

            return Optional.empty();
        }

        @Override
        public Page<Map<String, Object>> page(final Mappings mappings, final Set<Page.Stat> stats, final Page.Token token, final int count, final SearchResponse response) {

            return Page.empty();
        }
    }
}

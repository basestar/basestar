package io.basestar.storage.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.from.Join;
import io.basestar.schema.use.UseBoolean;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseOptional;
import io.basestar.schema.use.UseString;
import io.basestar.schema.util.Bucket;
import io.basestar.storage.TestStorage;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestQueryPlanner {

    @Test
    void testNonSplitAggregate() throws IOException {

        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        final ViewSchema viewSchema = namespace.requireViewSchema("USAddressStats");

        final QueryPlanner<SimpleStage> planner = new QueryPlanner.AggregateSplitting<>(true);

        final SimpleStage stage = planner.plan(new SimpleVisitor(), viewSchema, Expression.parse("count > 5"), ImmutableList.of(), ImmutableSet.of());

        final ObjectSchema sourceSchema = namespace.requireObjectSchema("Address");

        final SourceStage sourceStage = new SourceStage(sourceSchema);
        final ConformStage sourceConformStage = new ConformStage(sourceStage, sourceSchema);
        final FilterStage sourceFilterStage = new FilterStage(sourceConformStage, Expression.parse("country == 'United States'"));
        final AggStage aggStage = new AggStage(sourceFilterStage, ImmutableList.of("state"), ImmutableMap.of(
                "state", TypedExpression.from(Expression.parse("state"), UseOptional.from(UseString.DEFAULT)),
                "count", TypedExpression.from(Expression.parse("count(true)"), UseInteger.DEFAULT)
        ));
        final ConformStage conformStage = new ConformStage(aggStage, viewSchema);
        final FilterStage filterStage = new FilterStage(conformStage, Expression.parse("count > 5"));

        assertEquals(filterStage, stage);
    }

    @Test
    void testSplitAggregate() throws IOException {
        
        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        final ViewSchema viewSchema = namespace.requireViewSchema("AddressDisplayStats");
        
        final QueryPlanner<SimpleStage> planner = new QueryPlanner.AggregateSplitting<>(true);
        
        final SimpleStage stage = planner.plan(new SimpleVisitor(), viewSchema, Constant.TRUE, ImmutableList.of(), ImmutableSet.of());

        final ObjectSchema sourceSchema = namespace.requireObjectSchema("Address");

        final String zipDigest = "_" + Expression.parse("zip != null").digest();
        final String aggDigest = "_" + Expression.parse("count(zip != null)").digest();

        final SourceStage sourceStage = new SourceStage(sourceSchema);
        final ConformStage sourceConformStage = new ConformStage(sourceStage, sourceSchema);
        final FilterStage sourceFilterStage = new FilterStage(sourceConformStage, Expression.parse("country == 'United States'"));
        final MapStage preAggStage = new MapStage(sourceFilterStage, ImmutableMap.of(
                "state", TypedExpression.from(Expression.parse("state"), UseOptional.from(UseString.DEFAULT)),
                zipDigest, TypedExpression.from(Expression.parse("zip != null"), UseBoolean.DEFAULT)
        ));
        final AggStage aggStage = new AggStage(preAggStage, ImmutableList.of("state"), ImmutableMap.of(
                "state", TypedExpression.from(Expression.parse("state"), UseOptional.from(UseString.DEFAULT)),
                aggDigest, TypedExpression.from(Expression.parse("count(" + zipDigest + ")"), UseInteger.DEFAULT)
        ));
        final MapStage postAggStage = new MapStage(aggStage, ImmutableMap.of(
                "state", TypedExpression.from(Expression.parse("state"), UseOptional.from(UseString.DEFAULT)),
                "result", TypedExpression.from(Expression.parse("state + " + aggDigest), UseOptional.from(UseString.DEFAULT))
        ));
        final ConformStage conformStage = new ConformStage(postAggStage, viewSchema);

        assertEquals(conformStage, stage);
    }

    @Test
    void testNonAggregate() throws IOException {

        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        final ViewSchema viewSchema = namespace.requireViewSchema("GBAddresses");

        final QueryPlanner<SimpleStage> planner = new QueryPlanner.AggregateSplitting<>(true);
        final SimpleStage stage = planner.plan(new SimpleVisitor(), viewSchema, Expression.parse("state == 'Kent'"), ImmutableList.of(), ImmutableSet.of());

        final ObjectSchema sourceSchema = namespace.requireObjectSchema("Address");

        final SourceStage sourceStage = new SourceStage(sourceSchema);
        final ConformStage sourceConformStage = new ConformStage(sourceStage, sourceSchema);
        final FilterStage sourceFilterStage = new FilterStage(sourceConformStage, Expression.parse("country == 'United Kingdom'"));
        final MapStage sourceMapStage = new MapStage(sourceFilterStage, ImmutableMap.of(
                "country", TypedExpression.from(Expression.parse("country"), UseOptional.from(UseString.DEFAULT)),
                "state", TypedExpression.from(Expression.parse("state"), UseOptional.from(UseString.DEFAULT)),
                "city", TypedExpression.from(Expression.parse("city"), UseOptional.from(UseString.DEFAULT)),
                "zip", TypedExpression.from(Expression.parse("zip"), UseOptional.from(UseString.DEFAULT)),
                ViewSchema.ID, TypedExpression.from(Expression.parse(ReferableSchema.ID), UseString.DEFAULT)
        ));
        final ConformStage conformStage = new ConformStage(sourceMapStage, viewSchema);
        final FilterStage filterStage = new FilterStage(conformStage, Expression.parse("state == 'Kent'"));

        assertEquals(filterStage, stage);
    }

    interface SimpleStage {

        SimpleStage getInput();

        default Layout getLayout() {

            final SimpleStage input = getInput();
            if(input != null) {
                return input.getLayout();
            } else {
                throw new IllegalStateException();
            }
        }
    }

    @Data
    static class AggStage implements SimpleStage {

        private final SimpleStage input;
        
        private final List<String> group;
        
        private final Map<String, TypedExpression<?>> expressions;

        @Override
        public Layout getLayout() {

            return Layout.simple(Immutable.transformValues(expressions, (k, v) -> v.getType()));
        }
    }

    @Data
    static class EmptyStage implements SimpleStage {

        private final LinkableSchema schema;

        private final Set<Name> expand;

        @Override
        public Layout getLayout() {

            return Layout.simple(schema.getSchema(), expand);
        }

        @Override
        public SimpleStage getInput() {

            return null;
        }
    }

    @Data
    static class ExpandStage implements SimpleStage {

        private final SimpleStage input;

        private final LinkableSchema schema;

        private final Set<Name> expand;

        @Override
        public Layout getLayout() {

            return Layout.simple(schema.getSchema(), expand);
        }
    }

    @Data
    static class FilterStage implements SimpleStage {

        private final SimpleStage input;

        private final Expression condition;
    }

    @Data
    static class MapStage implements SimpleStage {

        private final SimpleStage input;

        private final Map<String, TypedExpression<?>> expressions;

        @Override
        public Layout getLayout() {

            return Layout.simple(Immutable.transformValues(expressions, (k, v) -> v.getType()));
        }
    }

    @Data
    static class SortStage implements SimpleStage {

        private final SimpleStage input;

        private final List<Sort> sort;
    }

    @Data
    static class SourceStage implements SimpleStage {

        private final LinkableSchema schema;

        @Override
        public Layout getLayout() {

            return schema;
        }

        @Override
        public SimpleStage getInput() {

            return null;
        }
    }

    @Data
    static class ConformStage implements SimpleStage {

        private final SimpleStage input;

        private final InstanceSchema schema;

        @Override
        public Layout getLayout() {

            return schema;
        }
    }

    private static class SimpleVisitor implements QueryStageVisitor<SimpleStage> {
    
        @Override
        public SimpleStage agg(final SimpleStage input, final List<String> group, final Map<String, TypedExpression<?>> expressions) {
            
            return new AggStage(input, group, expressions);
        }

        @Override
        public SimpleStage empty(final LinkableSchema schema, final Set<Name> expand) {
            
            return new EmptyStage(schema, expand);
        }

        @Override
        public SimpleStage expand(final SimpleStage input, final LinkableSchema schema, final Set<Name> expand, final Set<Bucket> buckets) {

            return new ExpandStage(input, schema, expand);
        }

        @Override
        public SimpleStage filter(final SimpleStage input, final Expression condition) {

            return new FilterStage(input, condition);
        }

        @Override
        public SimpleStage map(final SimpleStage input, final Map<String, TypedExpression<?>> expressions) {

            return new MapStage(input, expressions);
        }

        @Override
        public SimpleStage sort(final SimpleStage input, final List<Sort> sort) {

            return new SortStage(input, sort);
        }

        @Override
        public SimpleStage source(final LinkableSchema schema, final Set<Bucket> buckets) {

            return new SourceStage(schema);
        }

        @Override
        public SimpleStage union(final List<SimpleStage> inputs) {

            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleStage conform(final SimpleStage input, final InstanceSchema schema, final Set<Name> expand) {
            
            return new ConformStage(input, schema);
        }

        @Override
        public SimpleStage sql(final String sql, final InstanceSchema schema, final Map<String, SimpleStage> with) {

            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleStage join(final SimpleStage left, final SimpleStage right, final Join join) {

            throw new UnsupportedOperationException();
        }
    }
}

package io.basestar.storage.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.use.UseBoolean;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseOptional;
import io.basestar.schema.use.UseString;
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
        final ViewSchema viewSchema = namespace.requireViewSchema("AddressStats");

        final QueryPlanner<QueryStage> planner = new QueryPlanner.AggregateSplitting<>();

        final QueryStage stage = planner.plan(new SimpleVisitor(), viewSchema, Expression.parse("count > 5"), ImmutableList.of(), ImmutableSet.of());

        final ObjectSchema sourceSchema = namespace.requireObjectSchema("Address");

        final SourceStage sourceStage = new SourceStage(sourceSchema);
        final SchemaStage sourceSchemaStage = new SchemaStage(sourceStage, sourceSchema);
        final FilterStage sourceFilterStage = new FilterStage(sourceSchemaStage, Expression.parse("country == 'US'"));
        final AggStage aggStage = new AggStage(sourceFilterStage, ImmutableList.of("state"), ImmutableMap.of(
                "state", TypedExpression.from(Expression.parse("state"), UseOptional.from(UseString.DEFAULT)),
                "count", TypedExpression.from(Expression.parse("count(true)"), UseInteger.DEFAULT)
        ));
        final SchemaStage schemaStage = new SchemaStage(aggStage, viewSchema);
        final FilterStage filterStage = new FilterStage(schemaStage, Expression.parse("count > 5"));

        assertEquals(filterStage, stage);
    }

    @Test
    void testSplitAggregate() throws IOException {
        
        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        final ViewSchema viewSchema = namespace.requireViewSchema("AddressDisplayStats");
        
        final QueryPlanner<QueryStage> planner = new QueryPlanner.AggregateSplitting<>();
        
        final QueryStage stage = planner.plan(new SimpleVisitor(), viewSchema, Constant.TRUE, ImmutableList.of(), ImmutableSet.of());

        final ObjectSchema sourceSchema = namespace.requireObjectSchema("Address");

        final String zipDigest = "_" + Expression.parse("zip != null").digest();
        final String aggDigest = "_" + Expression.parse("count(zip != null)").digest();

        final SourceStage sourceStage = new SourceStage(sourceSchema);
        final SchemaStage sourceSchemaStage = new SchemaStage(sourceStage, sourceSchema);
        final FilterStage sourceFilterStage = new FilterStage(sourceSchemaStage, Expression.parse("country == 'US'"));
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
        final SchemaStage schemaStage = new SchemaStage(postAggStage, viewSchema);

        assertEquals(schemaStage, stage);
    }

    @Test
    void testNonAggregate() throws IOException {

        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        final ViewSchema viewSchema = namespace.requireViewSchema("GBAddresses");

        final QueryPlanner<QueryStage> planner = new QueryPlanner.AggregateSplitting<>();
        final QueryStage stage = planner.plan(new SimpleVisitor(), viewSchema, Expression.parse("state == 'Kent'"), ImmutableList.of(), ImmutableSet.of());

        final ObjectSchema sourceSchema = namespace.requireObjectSchema("Address");

        final SourceStage sourceStage = new SourceStage(sourceSchema);
        final SchemaStage sourceSchemaStage = new SchemaStage(sourceStage, sourceSchema);
        final FilterStage sourceFilterStage = new FilterStage(sourceSchemaStage, Expression.parse("country == 'GB'"));
        final MapStage sourceMapStage = new MapStage(sourceFilterStage, ImmutableMap.of(
                "country", TypedExpression.from(Expression.parse("country"), UseOptional.from(UseString.DEFAULT)),
                "state", TypedExpression.from(Expression.parse("state"), UseOptional.from(UseString.DEFAULT)),
                "city", TypedExpression.from(Expression.parse("city"), UseOptional.from(UseString.DEFAULT)),
                "zip", TypedExpression.from(Expression.parse("zip"), UseOptional.from(UseString.DEFAULT)),
                ViewSchema.ID, TypedExpression.from(Expression.parse(ReferableSchema.ID), UseString.DEFAULT)
        ));
        final SchemaStage schemaStage = new SchemaStage(sourceMapStage, viewSchema);
        final FilterStage filterStage = new FilterStage(schemaStage, Expression.parse("state == 'Kent'"));

        assertEquals(filterStage, stage);
    }

    interface SimpleStage extends QueryStage {

        QueryStage getInput();

        @Override
        default Layout getLayout() {

            final QueryStage input = getInput();
            if(input != null) {
                return input.getLayout();
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        default List<Sort> getSort() {

            final QueryStage input = getInput();
            if(input != null) {
                return input.getSort();
            } else {
                return ImmutableList.of();
            }
        }

        @Override
        default Expression getFilter() {

            final QueryStage input = getInput();
            if(input != null) {
                return input.getFilter();
            } else {
                return Constant.TRUE;
            }
        }
    }

    @Data
    static class AggStage implements QueryStage {

        private final QueryStage input;
        
        private final List<String> group;
        
        private final Map<String, TypedExpression<?>> expressions;

        @Override
        public Layout getLayout() {

            return Layout.simple(Immutable.transformValues(expressions, (k, v) -> v.getType()));
        }

        @Override
        public List<Sort> getSort() {

            return ImmutableList.of();
        }

        @Override
        public Expression getFilter() {

            return Constant.TRUE;
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
        public QueryStage getInput() {

            return null;
        }
    }

    @Data
    static class ExpandStage implements SimpleStage {

        private final QueryStage input;

        private final LinkableSchema schema;

        private final Set<Name> expand;

        @Override
        public Layout getLayout() {

            return Layout.simple(schema.getSchema(), expand);
        }
    }

    @Data
    static class FilterStage implements SimpleStage {

        private final QueryStage input;

        private final Expression condition;

        @Override
        public Expression getFilter() {

            return condition;
        }
    }

    @Data
    static class MapStage implements QueryStage {

        private final QueryStage input;

        private final Map<String, TypedExpression<?>> expressions;

        @Override
        public Layout getLayout() {

            return Layout.simple(Immutable.transformValues(expressions, (k, v) -> v.getType()));
        }

        @Override
        public List<Sort> getSort() {

            return ImmutableList.of();
        }

        @Override
        public Expression getFilter() {

            return Constant.TRUE;
        }
    }

    @Data
    static class SortStage implements SimpleStage {

        private final QueryStage input;

        private final List<Sort> sort;

        @Override
        public List<Sort> getSort() {

            return sort;
        }
    }

    @Data
    static class SourceStage implements SimpleStage {

        private final LinkableSchema schema;

        @Override
        public Layout getLayout() {

            return schema;
        }

        @Override
        public QueryStage getInput() {

            return null;
        }
    }

    @Data
    static class SchemaStage implements QueryStage {

        private final QueryStage input;

        private final InstanceSchema schema;

        @Override
        public Layout getLayout() {

            return schema;
        }

        @Override
        public List<Sort> getSort() {

            return input.getSort();
        }

        @Override
        public Expression getFilter() {

            return input.getFilter();
        }
    }

    private static class SimpleVisitor implements QueryStageVisitor<QueryStage> {
    
        @Override
        public QueryStage agg(final QueryStage input, final List<String> group, final Map<String, TypedExpression<?>> expressions) {
            
            return new AggStage(input, group, expressions);
        }

        @Override
        public QueryStage empty(final LinkableSchema schema, final Set<Name> expand) {
            
            return new EmptyStage(schema, expand);
        }

        @Override
        public QueryStage expand(final QueryStage input, final LinkableSchema schema, final Set<Name> expand) {

            return new ExpandStage(input, schema, expand);
        }

        @Override
        public QueryStage filter(final QueryStage input, final Expression condition) {

            return new FilterStage(input, condition);
        }

        @Override
        public QueryStage map(final QueryStage input, final Map<String, TypedExpression<?>> expressions) {

            return new MapStage(input, expressions);
        }

        @Override
        public QueryStage sort(final QueryStage input, final List<Sort> sort) {

            return new SortStage(input, sort);
        }

        @Override
        public QueryStage source(final LinkableSchema schema) {

            return new SourceStage(schema);
        }

        @Override
        public QueryStage union(final List<QueryStage> inputs) {

            throw new UnsupportedOperationException();
        }

        @Override
        public QueryStage conform(final QueryStage input, final InstanceSchema schema) {
            
            return new SchemaStage(input, schema);
        }
    }
}

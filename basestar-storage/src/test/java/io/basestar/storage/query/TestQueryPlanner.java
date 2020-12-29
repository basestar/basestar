package io.basestar.storage.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseOptional;
import io.basestar.schema.use.UseString;
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
    void testQueryPlanner() throws IOException {
        
        final Namespace namespace = Namespace.load(TestQueryPlanner.class.getResource("/schema/PetStore.yml"));
        final ViewSchema viewSchema = namespace.requireViewSchema("PetStats");
        
        final QueryPlanner<QueryStage> planner = new QueryPlanner.Default<>();
        
        final QueryStage stage = planner.plan(new SimpleVisitor(), viewSchema, Expression.parse("schema == 'Cat'"), ImmutableList.of(), ImmutableSet.of());

        final InterfaceSchema sourceSchema = namespace.requireInterfaceSchema("Pet");

        final String aggDigest = "_" + Expression.parse("count()").digest();

        final SourceStage sourceStage = new SourceStage(sourceSchema);
        final SchemaStage sourceSchemaStage = new SchemaStage(sourceStage, sourceSchema);
        final FilterStage sourceFilterStage = new FilterStage(sourceSchemaStage, Expression.parse("status == 'available'"));
        final MapStage sourceMapStage = new MapStage(sourceFilterStage, ImmutableMap.of(
                "schema", Expression.parse("schema")
        ), ImmutableMap.of(
                "schema", UseOptional.from(UseString.DEFAULT)
        ));
        final AggregateStage aggStage = new AggregateStage(sourceMapStage, ImmutableList.of("schema"), ImmutableMap.of(
                aggDigest, (Aggregate)Expression.parse("count(true)")
        ), ImmutableMap.of(
                "schema", UseOptional.from(UseString.DEFAULT),
                aggDigest, UseInteger.DEFAULT
        ));
        final MapStage mapStage = new MapStage(aggStage, ImmutableMap.of(
                "schema", Expression.parse("schema"),
                "count", Expression.parse(aggDigest)
        ), ImmutableMap.of(
                "count", UseOptional.from(UseInteger.DEFAULT),
                "schema", UseOptional.from(UseString.DEFAULT)
        ));
        final SchemaStage schemaStage = new SchemaStage(mapStage, viewSchema);
        final FilterStage filterStage = new FilterStage(schemaStage, Expression.parse("schema == 'Cat'"));

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
    static class AggregateStage implements QueryStage {

        private final QueryStage input;
        
        private final List<String> group;
        
        private final Map<String, Aggregate> aggregates;
        
        private final Map<String, Use<?>> output;

        @Override
        public Layout getLayout() {

            return Layout.simple(output);
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

        private final Map<String, Expression> expressions;

        private final Map<String, Use<?>> output;

        @Override
        public Layout getLayout() {

            return Layout.simple(output);
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
        public QueryStage aggregate(final QueryStage input, final List<String> group, final Map<String, Aggregate> aggregates, final Map<String, Use<?>> output) {
            
            return new AggregateStage(input, group, aggregates, output);
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
        public QueryStage map(final QueryStage input, final Map<String, Expression> expressions, final Map<String, Use<?>> output) {

            return new MapStage(input, expressions, output);
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
        public QueryStage schema(final QueryStage input, final InstanceSchema schema) {
            
            return new SchemaStage(input, schema);
        }
    }
}

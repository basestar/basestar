//package io.basestar.storage.sql.mapping;
//
//import com.google.common.collect.ImmutableSortedMap;
//import io.basestar.expression.Expression;
//import io.basestar.schema.Index;
//import io.basestar.schema.LinkableSchema;
//import io.basestar.schema.QuerySchema;
//import io.basestar.storage.sql.resolver.ColumnResolver;
//import io.basestar.storage.sql.resolver.ExpressionResolver;
//import io.basestar.storage.sql.resolver.TableResolver;
//import io.basestar.util.Name;
//import io.basestar.util.Sort;
//import lombok.Getter;
//import org.jooq.*;
//import org.jooq.impl.DSL;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.SortedMap;
//import java.util.function.Function;
//
//public class QuerySchemaMapping implements SchemaMapping {
//
//    @Getter
//    private final QuerySchema schema;
//
//    @Getter
//    private final SortedMap<String, PropertyMapping<?>> properties;
//
//    @Getter
//    private final SortedMap<String, LinkMapping> links;
//
//    private final SQLResolver sqlResolver;
//
//    public interface SQLResolver {
//
//        SQL sql(DSLContext context, TableResolver tableResolver, ExpressionResolver expressionResolver);
//    }
//
//    public QuerySchemaMapping(final QuerySchema schema, final Map<String, PropertyMapping<?>> properties, final Map<String, LinkMapping> links, final SQLResolver sqlResolver) {
//
//        this.schema = schema;
//        this.properties = ImmutableSortedMap.copyOf(properties);
//        this.links = ImmutableSortedMap.copyOf(links);
//        this.sqlResolver = sqlResolver;
//    }
//
//    @Override
//    public SelectJoinStep<Record> select(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver) {
//
//        final List<Field<?>> fields = selectFields(io.basestar.util.Name.of(), new ColumnResolver() {
//            @Override
//            public Optional<Field<?>> column(final Name name) {
//                return Optional.empty();
//            }
//        });
//        final SQL sql = sqlResolver.sql(context, tableResolver, expressionResolver);
//
//        return context.select(fields).from(sql);
//    }
//
//    @Override
//    public SelectConditionStep<Record1<Integer>> selectCount(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression) {
//
//        final SQL sql = sqlResolver.sql(context, tableResolver, expressionResolver);
//        return context.select(DSL.count()).from(sql).where(condition(context, tableResolver, expressionResolver, expression));
//    }
//
//    @Override
//    public List<OrderField<?>> orderFields(final DSLContext context, final TableResolver tableResolver, final List<Sort> sort) {
//
//        return null;
//    }
//
//    @Override
//    public Condition condition(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression) {
//
//        return null;
//    }
//}

package io.basestar.spark.database;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.basestar.expression.Expression;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Reserved;
import io.basestar.spark.transform.MarshallTransform;
import io.basestar.spark.transform.Transform;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public interface QueryChain<T> {

    Name DEFAULT_EXPAND = Name.of(Reserved.PREFIX + "default");

    Dataset<T> query(@Nullable Expression query, @Nonnull List<Sort> sort, @Nonnull Set<Name> expand);

    default Dataset<T> query(@Nullable final Expression query) {

        return query(query, ImmutableList.of(), ImmutableSet.of());
    }

    default Dataset<T> query() {

        return query(null);
    }

    default QueryChain<T> expand(@Nonnull final Name expand) {

        return expand(ImmutableSet.of(expand));
    }

    default QueryChain<T> expand(@Nonnull final Set<Name> expand) {

        return (query, sort, expand2) -> this.query(query, sort, Sets.union(expand, expand2));
    }

    default QueryChain<T> defaultExpand() {

        return expand(ImmutableSet.of(DEFAULT_EXPAND));
    }

    default QueryChain<T> sort(@Nonnull final Sort sort) {

        return sort(ImmutableList.of(sort));
    }

    default QueryChain<T> sort(@Nonnull final List<Sort> sort) {

        return (query, sort2, expand) -> this.query(query, ImmutableList.<Sort>builder().addAll(sort).addAll(sort2).build(), expand);
    }

    default <T2> QueryChain<T2> as(final Encoder<T2> encoder) {

//        final ConformTransform conform = ConformTransform.builder()
//                .structType(encoder.schema()).build();

        final QueryChain<T> self = this;
        return (query, sort, expand) -> self.query(query, sort, expand).toDF().as(encoder);
    }

    default <T2> QueryChain<T2> as(final Class<T2> marshallAs) {

        return as(new MappingContext(), marshallAs);
    }

    default <T2> QueryChain<T2> as(final MappingContext context, final Class<T2> marshallAs) {

        final QueryChain<T> self = this;
        final MarshallTransform<T2> marshall = MarshallTransform.<T2>builder().context(context).targetType(marshallAs).build();
        return (query, sort, expand) -> marshall.accept(self.query(query, sort, expand).toDF());
    }

    default <T2> QueryChain<T2> then(final Transform<Dataset<T>, Dataset<T2>> next) {

        return (query, sort, expand) -> next.accept(this.query(query, sort, expand));
    }
}

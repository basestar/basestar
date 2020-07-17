package io.basestar.spark.database;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.basestar.expression.Expression;
import io.basestar.spark.transform.ConformTransform;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public interface QueryChain<T> {

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

    default QueryChain<T> sort(@Nonnull final Sort sort) {

        return sort(ImmutableList.of(sort));
    }

    default QueryChain<T> sort(@Nonnull final List<Sort> sort) {

        return (query, sort2, expand) -> this.query(query, ImmutableList.<Sort>builder().addAll(sort).addAll(sort2).build(), expand);
    }

    default <T2> QueryChain<T2> as(final Encoder<T2> encoder) {

        final ConformTransform conform = ConformTransform.builder()
                .structType(encoder.schema()).build();

        final QueryChain<T> self = this;
        return (expand, query, sort) -> conform.accept(self.query(expand, query, sort).toDF()).as(encoder);
    }

    default <T2> QueryChain<T2> as(final Class<T2> beanClass) {

        return as(Encoders.bean(beanClass));
    }
}

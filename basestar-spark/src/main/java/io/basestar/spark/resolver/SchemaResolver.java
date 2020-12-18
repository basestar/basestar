package io.basestar.spark.resolver;

/*-
 * #%L
 * basestar-spark
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.ImmutableSet;
import io.basestar.schema.LinkableSchema;
import io.basestar.spark.combiner.Combiner;
import io.basestar.spark.source.Source;
import io.basestar.spark.transform.SchemaTransform;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.NamingConvention;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Deprecated
public interface SchemaResolver {

    Dataset<Row> resolve(LinkableSchema schema, Set<Name> expand);

    default Dataset<Row> resolve(final LinkableSchema schema) {

        return resolve(schema, ImmutableSet.of());
    }

    default Dataset<Row> resolveAndConform(final LinkableSchema schema, final Set<Name> expand) {

        return conform(schema, expand, resolve(schema, expand));
    }

    default Dataset<Row> conform(final LinkableSchema schema, final Set<Name> expand, final Dataset<Row> input) {

        final SchemaTransform schemaTransform = SchemaTransform.builder().schema(schema).expand(expand).build();
        return schemaTransform.accept(input);
    }

    default SchemaResolver then(final Transform<Dataset<Row>, Dataset<Row>> transform) {

        return (schema, expand) -> transform.accept(SchemaResolver.this.resolve(schema, expand));
    }

    default Source<Dataset<Row>> source(final LinkableSchema schema) {

        return source(schema, ImmutableSet.of());
    }

    default Source<Dataset<Row>> source(final LinkableSchema schema, final Set<Name> expand) {

        return sink -> sink.accept(resolve(schema, expand));
    }

    @RequiredArgsConstructor
    class Automatic implements SchemaResolver {

        private final SchemaResolver resolver;

        private final NamingConvention naming;

        public Automatic(final SchemaResolver resolver) {

            this(resolver, NamingConvention.DEFAULT);
        }

        @Override
        public Dataset<Row> resolve(final LinkableSchema schema, final Set<Name> expand) {

            return resolver.resolve(schema, expand);
        }

        //        protected Dataset<Row> object(final ObjectSchema schema, final Set<Name> expand) {
//
//            final Set<Name> mergedExpand = expand(schema, expand);
//            final SchemaTransform schemaTransform = SchemaTransform.builder().schema(schema).naming(naming).build();
//            final ExpressionTransform expressionTransform = ExpressionTransform.builder().schema(schema).expand(mergedExpand).build();
//            final ExpandTransform expandTransform = ExpandTransform.builder().resolver(this).schema(schema).expand(mergedExpand).build();
//            final Dataset<Row> base = Nullsafe.require(resolver.resolve(schema, schema.getExpand()));
//            return schemaTransform.then(expressionTransform).then(expandTransform).accept(base);
//        }
//
//        protected Dataset<Row> view(final ViewSchema schema, final Set<Name> expand) {
//
//            final Set<Name> mergedExpand = expand(schema, expand);
//            final ViewTransform viewTransform = ViewTransform.builder().schema(schema).build();
//            final ExpandTransform expandTransform = ExpandTransform.builder().resolver(this).schema(schema).expand(mergedExpand).build();
//            final ViewSchema.From from = schema.getFrom();
//            final Dataset<Row> base = resolve(from.getSchema(), from.getExpand());
//            return viewTransform.then(expandTransform).accept(base);
//        }
//
//        @Override
//        public Dataset<Row> resolve(final LinkableSchema schema, final Set<Name> expand) {
//
//            if(schema instanceof ObjectSchema) {
//                return object((ObjectSchema)schema, expand);
//            } else if(schema instanceof ViewSchema) {
//                return view((ViewSchema)schema, expand);
//            } else {
//                throw new IllegalStateException("Cannot resolve dataset for schema: " + schema.getQualifiedName());
//            }
//        }
//
//        protected Set<Name> expand(final InstanceSchema schema, final Set<Name> expand) {
//
//            if(expand.contains(QueryChain.DEFAULT_EXPAND)) {
//                final Set<Name> mergedExpand = new HashSet<>(expand);
//                mergedExpand.remove(QueryChain.DEFAULT_EXPAND);
//                mergedExpand.addAll(schema.getExpand());
//                return mergedExpand;
//            } else {
//                return expand;
//            }
//        }
    }

    class Combining implements SchemaResolver {

        private final SchemaResolver baseline;

        private final SchemaResolver overlay;

        private final Combiner combiner;

        private final String joinType;

        public Combining(final SchemaResolver baseline, final SchemaResolver overlay) {

            this(baseline, overlay, null);
        }

        public Combining(final SchemaResolver baseline, final SchemaResolver overlay, final Combiner combiner) {

            this(baseline, overlay, combiner, null);
        }

        public Combining(final SchemaResolver baseline, final SchemaResolver overlay, final Combiner combiner, final String joinType) {

            this.baseline = Nullsafe.require(baseline);
            this.overlay = Nullsafe.require(overlay);
            this.combiner = Nullsafe.orDefault(combiner, Combiner.SIMPLE);
            this.joinType = Nullsafe.orDefault(joinType, Combiner.DEFAULT_JOIN_TYPE);
        }

        @Override
        public Dataset<Row> resolve(final LinkableSchema schema, final Set<Name> expand) {

            final Dataset<Row> baseline = this.baseline.resolve(schema, expand);
            final Dataset<Row> overlay = this.overlay.resolve(schema, expand);
            return combiner.apply(schema, expand, baseline, overlay, joinType);
        }
    }

    @RequiredArgsConstructor
    class Caching implements SchemaResolver {

        private final Map<Key, Dataset<Row>> cache = new HashMap<>();

        private final SchemaResolver delegate;

        @Override
        public Dataset<Row> resolve(final LinkableSchema schema, final Set<Name> expand) {

            return cache.computeIfAbsent(new Key(schema.getQualifiedName(), expand), ignored -> {

                final Dataset<Row> result = delegate.resolve(schema, expand);
                return result.cache();
            });
        }

        @Data
        private static class Key {

            private final Name name;

            private final Set<Name> expand;
        }
    }
}

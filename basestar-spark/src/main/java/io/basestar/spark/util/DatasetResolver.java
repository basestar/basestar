package io.basestar.spark.util;

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
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ViewSchema;
import io.basestar.spark.transform.*;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Set;

public interface DatasetResolver extends Serializable {

    default Dataset<Row> resolve(final InstanceSchema schema) {

        return resolve(schema, ImmutableSet.of());
    }

    Dataset<Row> resolve(InstanceSchema schema, Set<Name> expand);

    default Dataset<Row> resolveAndConform(final InstanceSchema schema, final Set<Name> expand) {

        return conform(schema, expand, resolve(schema, expand));
    }

    default Dataset<Row> conform(final InstanceSchema schema, final Set<Name> expand, final Dataset<Row> input) {

        final StructType structType = SparkSchemaUtils.structType(schema, expand);
        final ConformTransform conformTransform = ConformTransform.builder().structType(structType).build();
        return conformTransform.accept(input);
    }

    static DatasetResolver.Automatic automatic(final Automatic.Resolver resolver) {

        return new Automatic(resolver);
    }

    @RequiredArgsConstructor
    class Automatic implements DatasetResolver {

        public interface Resolver extends Serializable {

            Dataset<Row> resolve(InstanceSchema schema);
        }

        private final Resolver resolver;

        @Override
        public Dataset<Row> resolve(final InstanceSchema schema, final Set<Name> expand) {

            if(schema instanceof ObjectSchema) {

                final ObjectSchema objectSchema = (ObjectSchema)schema;
                final ExpressionTransform expressionTransform = ExpressionTransform.builder().schema(objectSchema).expand(expand).build();
                final SchemaTransform schemaTransform = SchemaTransform.builder().schema(schema).build();
                final ExpandTransform expandTransform = ExpandTransform.builder().resolver(this).schema(schema).expand(expand).build();
                final Dataset<Row> base = Nullsafe.require(resolver.resolve(schema));
                return schemaTransform.then(expressionTransform).then(expandTransform).accept(base);

            } else if(schema instanceof ViewSchema) {

                final ViewSchema viewSchema = (ViewSchema)schema;
                final ViewTransform viewTransform = ViewTransform.builder().schema(viewSchema).build();
                final ExpandTransform expandTransform = ExpandTransform.builder().resolver(this).schema(schema).expand(expand).build();
                // FIXME: view has an implied internal expand which must be passed here
                final Dataset<Row> base = resolve(viewSchema.getFrom(), ImmutableSet.of());
                return viewTransform.then(expandTransform).accept(base);

            } else {

                throw new IllegalStateException("Cannot resolve dataset for schema: " + schema.getQualifiedName());
            }
        }
    }
}

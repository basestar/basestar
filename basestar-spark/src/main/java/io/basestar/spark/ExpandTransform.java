package io.basestar.spark;

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

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Link;
import io.basestar.schema.Property;
import io.basestar.schema.Transient;
import io.basestar.util.Name;
import lombok.Builder;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@Builder(builderClassName = "Builder")
public class ExpandTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    @NonNull
    private final Function<String, Dataset<Row>> sources;

    @NonNull
    private final InstanceSchema schema;

    @NonNull
    private final Set<Name> expand;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
            final Set<Name> branch = branches.get(entry.getKey());

        }
        if(schema instanceof Link.Resolver) {
            for(final Map.Entry<String, Link> entry : ((Link.Resolver)schema).getLinks().entrySet()) {
                final Set<Name> branch = branches.get(entry.getKey());
                if(branch != null) {

                }
            }
        }
        if(schema instanceof Transient.Resolver) {
            for (final Map.Entry<String, Transient> entry : ((Transient.Resolver) schema).getTransients().entrySet()) {
                final Set<Name> branch = branches.get(entry.getKey());
                if(branch != null) {

                }
            }
        }
        return null;
    }
}

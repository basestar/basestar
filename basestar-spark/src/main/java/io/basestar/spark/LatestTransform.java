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

import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.stream.Stream;

public class LatestTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final ObjectSchema schema;

    public LatestTransform(final ObjectSchema schema) {

        this.schema = schema;
    }

    @Override
    public Dataset<Row> apply(final Dataset<Row> df) {

        final WindowSpec window = Window.partitionBy(Reserved.SCHEMA, Reserved.ID);

        final Stream<String> names = Stream.concat(
                schema.metadataSchema().keySet().stream(),
                schema.getAllProperties().keySet().stream()
        );
        final Column[] cols = names
                .map(name -> functions.last(df.col(name)).over(window).as(name))
                .toArray(Column[]::new);

        return df.select(cols).dropDuplicates(Reserved.SCHEMA, Reserved.ID);
    }
}

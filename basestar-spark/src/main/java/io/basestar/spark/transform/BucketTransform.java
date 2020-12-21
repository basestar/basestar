package io.basestar.spark.transform;

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

import io.basestar.schema.Bucketing;
import io.basestar.schema.Reserved;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.util.Nullsafe;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Getter
public class BucketTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    public static final String DEFAULT_OUTPUT_COLUMN = Reserved.PREFIX + "bucket";

    private final Bucketing bucketing;

    private final String outputColumn;

    @lombok.Builder(builderClassName = "Builder")
    BucketTransform(final Bucketing bucketing, final String outputColumnName) {

        this.bucketing = Nullsafe.require(bucketing);
        this.outputColumn = Nullsafe.orDefault(outputColumnName, DEFAULT_OUTPUT_COLUMN);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final StructField field = SparkRowUtils.field(outputColumn, DataTypes.IntegerType);
        final StructType outputType = SparkRowUtils.append(input.schema(), field);
        final Bucketing bucketing = this.bucketing;

        return input.map(SparkUtils.map(row -> {

            final int bucket = bucketing.apply(n -> SparkRowUtils.get(row, n));
            return SparkRowUtils.append(row, field, bucket);

        }), RowEncoder.apply(outputType));
    }
}

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

import io.basestar.schema.Reserved;
import io.basestar.spark.util.BucketFunction;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Getter;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

@Getter
public class BucketTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    public static final String DEFAULT_OUTPUT_COLUMN = Reserved.PREFIX + "bucket";

    private final List<Name> inputNames;

    private final String outputColumn;

    private final BucketFunction bucketFunction;

    @lombok.Builder(builderClassName = "Builder")
    BucketTransform(final List<Name> inputNames, final String outputColumnName, final BucketFunction bucketFunction) {

        this.inputNames = Nullsafe.require(inputNames);
        this.outputColumn = Nullsafe.orDefault(outputColumnName, DEFAULT_OUTPUT_COLUMN);
        this.bucketFunction = Nullsafe.require(bucketFunction);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final StructField field = SparkRowUtils.field(outputColumn, DataTypes.StringType);
        final StructType outputType = SparkRowUtils.append(input.schema(), field);
        final List<Name> inputNames = this.inputNames;
        final BucketFunction bucketFunction = this.bucketFunction;

        return input.map((MapFunction<Row, Row>) row -> {

            final StringBuilder input1 = new StringBuilder();
            for(final Name name : inputNames) {
                input1.append(Nullsafe.map(SparkRowUtils.get(row, name), Object::toString));
            }
            final String bucket = bucketFunction.apply(input1.toString());
            return SparkRowUtils.append(row, field, bucket);

        }, RowEncoder.apply(outputType));
    }
}

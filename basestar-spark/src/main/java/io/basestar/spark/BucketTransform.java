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

import com.google.common.collect.ImmutableList;
import io.basestar.schema.Reserved;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class BucketTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private static final List<Name> DEFAULT_INPUT_NAMES = ImmutableList.of(Name.of(Reserved.ID));

    private final List<Name> inputNames;

    private final String outputColumnName;

    private final UserDefinedFunction bucket;

    @lombok.Builder(builderClassName = "Builder")
    BucketTransform(final List<Name> inputNames, final String outputColumnName, final BucketFunction bucketFunction) {

        this.inputNames = Nullsafe.option(inputNames, DEFAULT_INPUT_NAMES);
        this.outputColumnName = Nullsafe.require(outputColumnName);
        Nullsafe.require(bucketFunction);
        this.bucket = functions.udf(
                (UDF1<String, Object>) bucketFunction::apply,
                DataTypes.StringType
        );
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        // FIXME: need to handle nested paths
        final Column concat = functions.concat_ws("", inputNames.stream().map(Name::toString).map(input::col).toArray(Column[]::new));
        final Column bucketValue = bucket.apply(concat);
        return input.withColumn(outputColumnName, bucketValue);
    }

}

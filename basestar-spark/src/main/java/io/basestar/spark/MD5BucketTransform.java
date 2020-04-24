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
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.List;

public class MD5BucketTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<String> idColumns;

    private final String bucketColumnName;

    private final int len;

    @lombok.Builder(builderClassName = "Builder")
    MD5BucketTransform(final List<String> idColumns, final String bucketColumnName, final Integer len) {

        this.idColumns = Nullsafe.option(idColumns, ImmutableList.of(Reserved.ID));
        this.bucketColumnName = Nullsafe.require(bucketColumnName);
        this.len = Nullsafe.option(len, 1);
        if(this.len < 1) {
            throw new IllegalStateException("MD5 substring length must be at least 1");
        }
        if(this.len > 4) {
            throw new IllegalStateException("MD5 substring longer than 4 will create over 1M buckets");
        }
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Column concat = functions.concat_ws("", idColumns.stream().map(input::col).toArray(Column[]::new));
        final Column md5 = functions.md5(concat);
        final Column substr = md5.substr(0, len);
        return input.withColumn(bucketColumnName, substr);
    }
}

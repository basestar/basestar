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
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class LatestVersionTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<String> idColumns;

    private final String versionColumn;

    @lombok.Builder(builderClassName = "Builder")
    LatestVersionTransform(final List<String> idColumns, final String versionColumn)  {

        this.idColumns = Nullsafe.option(idColumns, ImmutableList.of(Reserved.ID));
        this.versionColumn = Nullsafe.option(versionColumn, Reserved.VERSION);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> df) {

        final WindowSpec window = Window.partitionBy(idColumns.stream().map(df::col).toArray(Column[]::new))
                .orderBy(df.col(versionColumn).desc());

        final Column[] cols = Arrays.stream(df.schema().names())
                .map(name -> functions.first(df.col(name)).over(window).as(name))
                .toArray(Column[]::new);

        return df.select(cols).dropDuplicates(idColumns.toArray(new String[0]));
    }
}

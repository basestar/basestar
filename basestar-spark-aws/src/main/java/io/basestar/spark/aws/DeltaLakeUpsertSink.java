package io.basestar.spark.aws;

/*-
 * #%L
 * basestar-spark-aws
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
import io.basestar.spark.Sink;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.BiFunction;

public class DeltaLakeUpsertSink implements Sink<Dataset<Row>> {

    private final DeltaTable dt;

    private final BiFunction<DeltaTable, Dataset<Row>, Column> condition;

    public DeltaLakeUpsertSink(final DeltaTable dt) {

        this(dt, Reserved.ID);
    }

    public DeltaLakeUpsertSink(final DeltaTable dt, final String id) {

        this(dt, (t, df) -> t.toDF().col(id).equalTo(df.col(id)));
    }

    public DeltaLakeUpsertSink(final DeltaTable dt, final BiFunction<DeltaTable, Dataset<Row>, Column> condition) {

        this.dt = dt;
        this.condition = condition;
    }

    @Override
    public void accept(final Dataset<Row> df) {

        dt.merge(df, condition.apply(dt, df))
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();
        dt.generate("symlink_format_manifest");
    }
}

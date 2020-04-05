package io.basestar.spark.aws;

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

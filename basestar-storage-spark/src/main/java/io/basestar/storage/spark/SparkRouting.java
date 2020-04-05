package io.basestar.storage.spark;

import io.basestar.schema.ObjectSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface SparkRouting {

    Dataset<Row> objectRead(SparkSession session, ObjectSchema schema);

    Dataset<Row> historyRead(SparkSession session, ObjectSchema schema);
}

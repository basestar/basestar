package io.basestar.spark.aws;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.basestar.spark.Sink;
import io.basestar.spark.Source;
import lombok.Data;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

@Data
public class DynamoDBSource implements Source<RDD<Map<String, AttributeValue>>> {

    private final SparkSession session;

    private final String tableName;

    private final int minPartitions;

    @Override
    public void sink(final Sink<RDD<Map<String, AttributeValue>>> sink) {

        final SparkContext sc = session.sparkContext();
        final JobConf jobConf = new JobConf(sc.hadoopConfiguration());
        jobConf.set("dynamodb.input.tableName", tableName);
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat");

        sink.accept(sc.hadoopRDD(jobConf, DynamoDBInputFormat.class, Text.class, DynamoDBItemWritable.class, minPartitions)
                .toJavaRDD().map(v -> {
                    final DynamoDBItemWritable item = v._2();
                    return item.getItem();
                }).rdd());
    }
}

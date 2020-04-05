package io.basestar.spark.aws;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.basestar.spark.Sink;
import lombok.Data;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Map;

@Data
public class DynamoDBSink implements Sink<RDD<Map<String, AttributeValue>>> {

    private final String tableName;

    @Override
    public void accept(final RDD<Map<String, AttributeValue>> input) {

        final SparkContext sc = input.sparkContext();

        final JobConf jobConf = new JobConf(sc.hadoopConfiguration());
        jobConf.set("dynamodb.output.tableName", tableName);

        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat");

        input.toJavaRDD().<Text,  DynamoDBItemWritable>mapToPair(values -> {
            final DynamoDBItemWritable item = new DynamoDBItemWritable();
            item.setItem(values);
            return Tuple2.apply(new Text(""), item);
        }).saveAsHadoopDataset(jobConf);
    }
}

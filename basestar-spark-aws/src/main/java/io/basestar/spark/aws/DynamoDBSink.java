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

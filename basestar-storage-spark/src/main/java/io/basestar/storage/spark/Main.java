package io.basestar.storage.spark;

/*-
 * #%L
 * basestar-storage-spark
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.collect.ImmutableList;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.schema.Property;
import io.basestar.schema.StructSchema;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseString;
import io.basestar.spark.CriteriaSink;
import io.basestar.spark.SparkUtils;
import io.basestar.spark.StreamSource;
import io.basestar.spark.aws.*;
import io.delta.tables.DeltaTable;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.dynamodb.DynamoDBInputDStream;

import java.io.Serializable;

public class Main {

    public enum Action {

        UPSERT,
        DELETE;

        public static Action fromEventName(final String name) {

            switch(name.toUpperCase()) {
                case "REMOVE":
                    return DELETE;
                case "INSERT":
                case "MODIFY":
                default:
                    return UPSERT;
            }
        }
    }

    @Data
    public static class SchemaAction implements Serializable {

        private final Action action;

        private final String schema;

        public static SchemaAction from(final Record v) {

            final Action action = Action.fromEventName(v.getEventName());
            final StreamRecord streamRecord = v.getDynamodb();
            if(streamRecord.getNewImage() != null) {
                return new SchemaAction(action, DynamoDBSparkUtils.schema(streamRecord.getNewImage()));
            } else {
                return new SchemaAction(action, DynamoDBSparkUtils.schema(streamRecord.getOldImage()));
            }
        }
    }

    public static void main(final String[] args) {

        final Namespace ns = Namespace.builder()
                .setSchema("Version", StructSchema.builder()
                        .setProperty("id", Property.builder()
                                .setType(UseString.DEFAULT))
                        .setProperty("version", Property.builder()
                                .setType(UseInteger.DEFAULT))
                ).build();

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        final StreamingContext streamingContext = new StreamingContext(session.sparkContext(), Duration.apply(30 * 1000));

        final String path = "s3a://dlakestreams/";

        final String streamArn = "arn:aws:dynamodb:us-east-1:603553446988:table/basestar-prod-object/stream/2020-04-04T16:02:17.489";

        final ReceiverInputDStream<Record> input = DynamoDBInputDStream.builder()
                .streamingContext(streamingContext)
                .streamName(streamArn)
                .regionName("us-east-1")
                .initialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .checkpointInterval(new Duration(30 * 1000))
                .checkpointAppName("sample5")
                .build();

        new StreamSource<>(input)
                .sink(new CriteriaSink<>(
                        SchemaAction::from,
                        c -> {
                            final InstanceSchema schema = ns.requireInstanceSchema(c.getSchema());
                            final StructType structType = SparkUtils.structType(schema);
                            final String dtPath = path + c.getSchema();
                            if(!DeltaTable.isDeltaTable(dtPath)) {
                                System.err.println("Creating delta table " + dtPath);
                                final Dataset<Row> init = session.sqlContext().createDataFrame(ImmutableList.of(), structType);
                                init.write().format("delta").mode(SaveMode.Append).save(dtPath);
                            }
                            final DeltaTable dt = DeltaTable.forPath(dtPath);
                            if (c.getAction() == Action.UPSERT) {
                                System.err.println("Upsert into " + dtPath);
                                return new DynamoDBNewImageTransform()
                                        .then(new DynamoDBInputTransform(schema, structType))
                                        .then(new DeltaLakeUpsertSink(dt));
                            } else {
                                System.err.println("Delete from " + dtPath);
                                return new DynamoDBOldImageTransform()
                                        .then(new DynamoDBInputTransform(schema, structType))
                                        .then(new DeltaLakeDeleteSink(dt));
                            }
                }));

        System.err.println("starting");
        streamingContext.start();
        System.err.println("waiting");
        streamingContext.awaitTermination();
    }
}

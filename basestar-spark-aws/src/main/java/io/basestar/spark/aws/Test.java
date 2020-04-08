//package io.basestar.spark.aws;

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
//
//import com.google.common.collect.ImmutableList;
//import io.basestar.schema.Instance;
//import io.basestar.schema.Namespace;
//import io.basestar.schema.ObjectSchema;
//import io.basestar.schema.Reserved;
//import io.basestar.spark.GenericSink;
//import io.basestar.spark.Source;
//import io.basestar.spark.SparkUtils;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.types.StructType;
//
//import java.time.LocalDateTime;
//import java.util.*;
//
//public class Test {
//
//    public static void main(final String[] args) {
//
//        final SparkSession session = SparkSession.builder()
//                .master("local")
//                .config("fs.s3a.endpoint", "http://localhost:4572")
//                .config("fs.s3a.access.key", "AAA")
//                .config("fs.s3a.secret.key", "ZZZ")
//                .config("fs.s3a.path.style.access", "true")
//                .getOrCreate();
//
//        final Namespace ns = Namespace.builder()
//                .setSchema("Test", ObjectSchema.builder())
//                .build();
//
//        (new Source<Row>() {
//
//            @Override
//            public Dataset<Row> source(final SparkSession session) {
//
//                final ObjectSchema schema = ns.requireObjectSchema("Test");
//                final StructType structType = SparkUtils.schema(schema);
//                return session.createDataFrame(records(schema, structType), structType);
//            }
//
//            private List<Row> records(final ObjectSchema schema, final StructType structType) {
//
//                final List<Row> rows = new ArrayList<>();
//                for(int i = 0; i != 10; ++i) {
//                    final Map<String, Object> data = new HashMap<>();
//                    Instance.setId(data, UUID.randomUUID().toString());
//                    Instance.setSchema(data, schema.getName());
//                    Instance.setVersion(data, schema.getVersion());
//                    Instance.setCreated(data, LocalDateTime.now());
//                    Instance.setUpdated(data, LocalDateTime.now());
//                    final Instance instance = schema.create(data);
//                    rows.add(SparkUtils.toSpark(schema, structType, instance));
//                }
//                return rows;
//            }
//
//        }).chain().sink(GenericSink.builder()
//                .path("/Users/mattevans/basestar/basestar-new/basestar/basestar-spark-aws/target/parquet")
//                .prefixes(ImmutableList.of(
//                        GenericSink.Prefix.of(Reserved.ID, 2)
//                ))
//                .build()).apply(session);
//    }
//}

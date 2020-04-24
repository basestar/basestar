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

import io.basestar.spark.MD5BucketTransform;
import io.basestar.spark.PartitionedUpsertSink;
import io.basestar.spark.PartitionedUpsertSource;
import lombok.Data;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class S3LiveTest {

    @Data
    public static class TestRecord implements Serializable {

        private final String id;

        private final List<String> random;
    }

    @Test
    @Disabled
    public void test() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .config("spark.hadoop.dfs.replication", 1)
                .enableHiveSupport()
                .getOrCreate();

        final String MD5_COL = "__md5";

        final String initUri = "file:/Users/mattevans/basestar/basestar-new/basestar/basestar-spark-aws/target/init";

        session.sql("CREATE DATABASE IF NOT EXISTS test").collect();

        final List<TestRecord> records = new ArrayList<>();
        for (int j = 0; j != 50000; ++j) {
            final String id = RandomStringUtils.randomAlphanumeric(16);
            final List<String> random = new ArrayList<>();
            for(int k = 0; k != 100; ++k)  {
                random.add(RandomStringUtils.randomAlphanumeric(16));
            }
            records.add(new TestRecord(id, random));
        }
        final Dataset<Row> init = session.createDataFrame(records, TestRecord.class);
        MD5BucketTransform.builder().bucketColumnName(MD5_COL).build()
                .accept(init).write().mode(SaveMode.Overwrite).partitionBy(MD5_COL).parquet(initUri);

        session.sql("CREATE EXTERNAL TABLE IF NOT EXISTS test.init (id STRING, random ARRAY<STRING>) PARTITIONED BY(__md5 STRING) STORED AS PARQUET LOCATION \"" + initUri + "\" TBLPROPERTIES (\"parquet.compress\"=\"SNAPPY\")").collect();
        session.sql("MSCK REPAIR TABLE test.init");

        final PartitionedUpsertSource source = new PartitionedUpsertSource(session, "test", "init");

        final String table = "db";

        final URI tempUri = URI.create("file:/Users/mattevans/basestar/basestar-new/basestar/basestar-spark-aws/target/db");

        session.sql("CREATE EXTERNAL TABLE IF NOT EXISTS test." + table + " (id STRING, random ARRAY<STRING>) PARTITIONED BY(__md5 STRING) STORED AS PARQUET LOCATION \"" + tempUri + "\" TBLPROPERTIES (\"parquet.compress\"=\"SNAPPY\")").collect();

        source.then(PartitionedUpsertSink.builder().databaseName("test").tableName(table).build());

        System.err.println(session.sql("SELECT COUNT(*) FROM test." + table).collectAsList());
    }
}

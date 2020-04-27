//package io.basestar.spark.aws;
//
//
//import io.basestar.spark.BucketFunction;
//import io.basestar.spark.BucketTransform;
//import io.basestar.spark.PartitionedUpsertSink;
//import io.basestar.spark.PartitionedUpsertSource;
//import lombok.Data;
//import org.apache.commons.lang.RandomStringUtils;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.SparkSession;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.io.Serializable;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.List;
//
//public class S3LiveTest {
//
//    @Data
//    public static class TestRecord implements Serializable {
//
//        private final String id;
//
//        private final List<String> random;
//    }
//
//    @Test
//    @Disabled
//    public void test() throws IOException {
//
//        final SparkSession session = SparkSession.builder()
//                .master("local[*]")
//                .config("spark.hadoop.dfs.replication", 1)
//                .enableHiveSupport()
//                .getOrCreate();
//
//        final String MD5_COL = "__md5";
//
//        final String initUri = "file:/Users/mattevans/basestar/basestar-new/basestar/basestar-spark-aws/target/init";
//
//        session.sql("CREATE DATABASE IF NOT EXISTS test").collect();
//
//        final List<TestRecord> records = new ArrayList<>();
//        for (int j = 0; j != 50000; ++j) {
//            final String id = RandomStringUtils.randomAlphanumeric(16);
//            final List<String> random = new ArrayList<>();
//            for(int k = 0; k != 100; ++k)  {
//                random.add(RandomStringUtils.randomAlphanumeric(16));
//            }
//            records.add(new TestRecord(id, random));
//        }
//        final Dataset<Row> init = session.createDataFrame(records, TestRecord.class);
//        BucketTransform.builder().outputColumnName(MD5_COL).bucketFunction(BucketFunction.md5Prefix(2)).build()
//                .accept(init).write().mode(SaveMode.Overwrite).partitionBy(MD5_COL).parquet(initUri);
//
//        session.sql("CREATE EXTERNAL TABLE IF NOT EXISTS test.init (id STRING, random ARRAY<STRING>) PARTITIONED BY(__md5 STRING) STORED AS PARQUET LOCATION \"" + initUri + "\" TBLPROPERTIES (\"parquet.compress\"=\"SNAPPY\")").collect();
//        session.sql("MSCK REPAIR TABLE test.init");
//
//        final PartitionedUpsertSource source = new PartitionedUpsertSource(session, "test", "init");
//
//        final String table = "db";
//
//        final URI tempUri = URI.create("file:/Users/mattevans/basestar/basestar-new/basestar/basestar-spark-aws/target/db");
//
//        session.sql("CREATE EXTERNAL TABLE IF NOT EXISTS test." + table + " (id STRING, random ARRAY<STRING>) PARTITIONED BY(__md5 STRING) STORED AS PARQUET LOCATION \"" + tempUri + "\" TBLPROPERTIES (\"parquet.compress\"=\"SNAPPY\")").collect();
//
//        source.then(PartitionedUpsertSink.builder().databaseName("test").tableName(table).build());
//
//        System.err.println(session.sql("SELECT COUNT(*) FROM test." + table).collectAsList());
//    }
//}

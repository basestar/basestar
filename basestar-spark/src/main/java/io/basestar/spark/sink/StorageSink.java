//package io.basestar.spark.sink;
//
//import io.basestar.schema.Consistency;
//import io.basestar.schema.Instance;
//import io.basestar.schema.ObjectSchema;
//import io.basestar.schema.ReferableSchema;
//import io.basestar.spark.util.SparkSchemaUtils;
//import io.basestar.storage.Storage;
//import io.basestar.storage.Versioning;
//import lombok.RequiredArgsConstructor;
//import org.apache.spark.api.java.function.ForeachPartitionFunction;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import scala.Tuple2;
//
//import java.io.Serializable;
//import java.util.Map;
//
//@RequiredArgsConstructor
//public class StorageSink implements Sink<Dataset<Tuple2<Row, Row>>> {
//
//    private final ObjectSchema schema;
//
//    private final StorageSupplier supplier;
//
//    private final Consistency consistency;
//
//    private final Versioning versioning;
//
//    private final int batchSize;
//
//    public interface StorageSupplier extends Serializable {
//
//        Storage storage(ReferableSchema schema);
//    }
//
//    @Override
//    public void accept(final Dataset<Tuple2<Row, Row>> input) {
//
//        input.foreachPartition((ForeachPartitionFunction<Tuple2<Row, Row>>) iterator -> {
//
//            final Storage storage = supplier.storage(schema);
//
//            Storage.WriteTransaction transaction = null;
//            int size = 0;
//            while(iterator.hasNext()) {
//                final Tuple2<Row, Row> pair = iterator.next();
//                if(transaction == null) {
//                    transaction = storage.write(consistency, versioning);
//                }
//                if(pair._1() != null) {
//                    final Map<String, Object> before = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), pair._1());
//                    final String id = Instance.getId(before);
//                    if(pair._2() != null) {
//                        final Map<String, Object> after = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), pair._2());
//                        transaction.updateObject(schema, id, before, after);
//                    } else {
//                        transaction.deleteObject(schema, id, before);
//                    }
//                } else if(pair._2() != null) {
//                    final Map<String, Object> after = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), pair._2());
//                    final String id = Instance.getId(after);
//                    transaction.createObject(schema, id, after);
//                }
//                if(++size >= batchSize) {
//                    transaction.write().join();
//                    transaction = null;
//                    size = 0;
//                }
//            }
//            if(transaction != null) {
//                transaction.write().join();
//            }
//        });
//    }
//}

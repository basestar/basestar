package io.basestar.spark;

import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class StreamSource<O> implements Source<RDD<O>> {

    private final ReceiverInputDStream<O> input;

    public StreamSource(final ReceiverInputDStream<O> input) {

        this.input = input;
    }

    @Override
    public void sink(final Sink<RDD<O>> sink) {

        input.foreachRDD(new AbstractFunction1<RDD<O>, BoxedUnit>() {
            @Override
            public BoxedUnit apply(final RDD<O> rdd) {

                sink.accept(rdd);
                return null;
            }
        });
    }
}

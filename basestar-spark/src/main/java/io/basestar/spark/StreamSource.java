package io.basestar.spark;

/*-
 * #%L
 * basestar-spark
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

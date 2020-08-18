package io.basestar.spark.sink;

/*-
 * #%L
 * basestar-spark
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

import io.basestar.util.Nullsafe;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GroupingSink<T extends Serializable, G extends Serializable> implements Sink<RDD<T>> {

    private final GroupFunction<T, G> groupFunction;

    private final SinkFunction<T, G> sinkFunction;

    private final int threads;

    @lombok.Builder(builderClassName = "Builder")
    GroupingSink(final GroupFunction<T, G> groupFunction, final SinkFunction<T, G> sinkFunction, final Integer threads) {

        this.groupFunction = groupFunction;
        this.sinkFunction = sinkFunction;
        this.threads = Nullsafe.orDefault(threads, 1);
    }

    @Override
    public void accept(final RDD<T> input) {

        final SparkContext context = input.sparkContext();
        final JavaRDD<T> cached = input.toJavaRDD().cache();
        final ExecutorService executor;
        if(threads <= 1) {
            executor = Executors.newSingleThreadExecutor();
        } else {
            executor = Executors.newFixedThreadPool(threads);
        }
        try {
            final List<G> groups = cached.map(groupFunction::apply).distinct().collect();
            final List<CompletableFuture<?>> futures = new ArrayList<>();
            for (final G group : groups) {
                futures.add(CompletableFuture.supplyAsync(() -> {
                    context.setJobDescription("Group: " + group);
                    final RDD<T> groupRdd = cached.filter(v -> group.equals(groupFunction.apply(v))).rdd();
                    sinkFunction.accept(group, groupRdd);
                    return null;
                }, executor));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            context.setJobDescription(null);
        } finally {
            cached.unpersist(true);
            executor.shutdownNow();
        }
    }

    public interface GroupFunction<T, G> extends Serializable {

        G apply(T value);
    }

    public interface SinkFunction<T, G> extends Serializable {

        void accept(G group, RDD<T> rdd);
    }
}

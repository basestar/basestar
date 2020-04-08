package io.basestar.spark;

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

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.List;

@RequiredArgsConstructor
public class CriteriaSink<T, C> implements Sink<RDD<T>> {

    private final Criteria<T, C> criteria;

    private final Selector<T, C> selector;

    @Override
    public void accept(final RDD<T> input) {

        final JavaRDD<T> rdd = input.toJavaRDD().cache();
        final List<C> all = rdd.map(criteria::apply).distinct().collect();

        for (final C c : all) {
            selector.apply(c).accept(rdd.filter(v -> c.equals(criteria.apply(v))).rdd());
        }
    }

    public interface Criteria<T, C> extends Serializable {

        C apply(T value);
    }

    public interface Selector<T, C> extends Serializable {

        Sink<RDD<T>> apply(C criteria);
    }
}

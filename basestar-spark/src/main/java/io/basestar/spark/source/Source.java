package io.basestar.spark.source;

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

import io.basestar.spark.sink.Sink;
import io.basestar.spark.transform.Transform;

public interface Source<I> {

    void then(Sink<I> sink);

    default <O> Source<O> then(final Transform<I, O> next) {

        return sink -> then(next.then(sink));
    }

    static <I> Source<I> of(final I init) {

        return sink -> sink.accept(init);
    }
}

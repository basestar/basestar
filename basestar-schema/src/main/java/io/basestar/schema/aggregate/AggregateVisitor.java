package io.basestar.schema.aggregate;

/*-
 * #%L
 * basestar-schema
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

public interface AggregateVisitor<T> {

    T visitSum(Sum aggregate);

    T visitMin(Min aggregate);

    T visitMax(Max aggregate);

    T visitAvg(Avg aggregate);

    T visitCount(Count count);

    interface Defaulting<T> extends AggregateVisitor<T> {

        T visitDefault(Aggregate aggregate);

        @Override
        default T visitSum(final Sum aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitMin(final Min aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitMax(final Max aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitAvg(final Avg aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitCount(final Count aggregate) {

            return visitDefault(aggregate);
        }
    }
}

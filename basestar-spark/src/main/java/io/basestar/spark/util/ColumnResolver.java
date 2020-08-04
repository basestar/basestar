package io.basestar.spark.util;

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

import io.basestar.util.Name;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

import java.io.Serializable;

public interface ColumnResolver<R> extends Serializable {

    Column resolve(Dataset<R> input, Name name);

    static <T> ColumnResolver<T> lowercase(final ColumnResolver<T> delegate) {

        return (input, name) -> delegate.resolve(input, name.toLowerCase());
    }

    static <T> ColumnResolver<T> uppercase(final ColumnResolver<T> delegate) {

        return (input, name) -> delegate.resolve(input, name.toLowerCase());
    }

    static <T> Column nested(final Dataset<T> input, final Name name) {

        return nested(input.col(name.first()), name.withoutFirst());
    }

    static Column nested(final Column column, final Name name) {

        if(name.isEmpty()) {
            return column;
        } else {
            return nested(column.getField(name.first()), name.withoutFirst());
        }
    }
}

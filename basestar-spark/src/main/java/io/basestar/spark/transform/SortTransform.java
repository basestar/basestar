package io.basestar.spark.transform;

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

import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

import java.util.List;

public class SortTransform<T> implements Transform<Dataset<T>, Dataset<T>> {

    private final List<Sort> sort;

    private final ColumnResolver<T> columnResolver;

    @lombok.Builder(builderClassName = "Builder")
    SortTransform(final List<Sort> sort, final ColumnResolver<T> columnResolver) {

        this.sort = Nullsafe.require(sort);
        this.columnResolver = Nullsafe.orDefault(columnResolver, ColumnResolver::nested);
    }

    @Override
    public Dataset<T> accept(final Dataset<T> input) {

        final List<Sort> sort = this.sort;
        final ColumnResolver<T> columnResolver = this.columnResolver;
        return input.sort(sort.stream()
                .map(v -> SparkRowUtils.order(columnResolver.resolve(input, v.getName()), v.getOrder(), v.getNulls()))
                .toArray(Column[]::new));
    }
}

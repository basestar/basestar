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

import io.basestar.spark.util.SparkRowUtils;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;


/**
 * Force the input to match the field order in the provided schema.
 */

public class ConformTransform implements DatasetMapTransform {

    private final RowTransformImpl rowTransform;

    @lombok.Builder(builderClassName = "Builder")
    public ConformTransform(final StructType structType) {

        this.rowTransform = new RowTransformImpl(structType);
    }

    @Override
    public RowTransform rowTransform() {

        return rowTransform;
    }

    @AllArgsConstructor
    private static class RowTransformImpl implements RowTransform {

        private final StructType structType;

        @Override
        public StructType schema(final StructType input) {

            return structType;
        }

        @Override
        public Row accept(final Row input) {

            return SparkRowUtils.conform(input, structType);
        }
    }
}

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

import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import lombok.Builder;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

// FIXME: all functionality can be moved to SchemaTransform/ExpressionTransform/ConformTransform

@Builder
public class InstanceTransform implements Transform<Dataset<Row>, RDD<Map<String, Object>>> {

    private final InstanceSchema schema;

    @Override
    public RDD<Map<String, Object>> accept(final Dataset<Row> input) {

        return input.toJavaRDD().map(row -> {
            final Map<String, Object> instance = new HashMap<>(SparkSchemaUtils.fromSpark(schema, row));
            if(Instance.getCreated(instance) == null) {
                Instance.setCreated(instance, LocalDateTime.now());
            }
            if(Instance.getUpdated(instance) == null) {
                Instance.setUpdated(instance, LocalDateTime.now());
            }
            if(Instance.getVersion(instance) == null) {
                Instance.setVersion(instance, 1L);
            }
            return instance;
        }).rdd();
    }
}

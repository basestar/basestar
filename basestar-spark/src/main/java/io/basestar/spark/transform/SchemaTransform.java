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

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.use.Use;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Slf4j
public class SchemaTransform implements DatasetMapTransform {

    private final RowTransformImpl rowTransform;

    @lombok.Builder(builderClassName = "Builder")
    SchemaTransform(final InstanceSchema schema, final Set<Name> expand, final Map<String, Use<?>> extraMetadata, @Nullable final StructType structType) {

        this.rowTransform = new RowTransformImpl(schema, expand, extraMetadata, structType);
    }

    @Override
    public RowTransform rowTransform() {

        return rowTransform;
    }

    private static class RowTransformImpl implements RowTransform {

        private final InstanceSchema schema;

        private final Set<Name> expand;

        private final Map<String, Use<?>> extraMetadata;

        private final StructType structType;

        RowTransformImpl(final InstanceSchema schema, final Set<Name> expand, final Map<String, Use<?>> extraMetadata, @Nullable final StructType structType) {

            this.schema = Nullsafe.require(schema);
            if(expand != null) {
                this.expand = expand;
            } else if(this.schema instanceof LinkableSchema) {
                this.expand = ((LinkableSchema) this.schema).getExpand();
            } else {
                this.expand = Collections.emptySet();
            }
            this.extraMetadata = Nullsafe.orDefault(extraMetadata);
            this.structType = Nullsafe.orDefault(structType, () -> SparkSchemaUtils.structType(this.schema, this.expand, this.extraMetadata));
        }

        @Override
        public StructType schema(final StructType input) {

            return structType;
        }

        @Override
        public Row accept(final Row input) {

            final Map<String, Object> object = schema.create(SparkSchemaUtils.fromSpark(schema, expand, input), expand, true);
            return SparkSchemaUtils.toSpark(schema, expand, extraMetadata, structType, object);
        }
    }
}

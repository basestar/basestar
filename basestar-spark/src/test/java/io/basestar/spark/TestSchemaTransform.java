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

import com.google.common.collect.ImmutableList;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.source.Source;
import io.basestar.spark.transform.ConformTransform;
import io.basestar.spark.transform.SchemaTransform;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSchemaTransform extends AbstractSparkTest {

    @Test
    public void testSchemaTransform() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final ObjectSchema a = namespace.requireObjectSchema("A");

        final Source<Dataset<Row>> sourceA = (Source<Dataset<Row>>) sink -> sink.accept(session.createDataset(ImmutableList.of(
                new A("a:1", new B("b:1", null, 0L))
        ), Encoders.bean(A.class)).toDF());

        final SchemaTransform schema = SchemaTransform.builder().schema(a).build();

        final Encoder<A> encoder = Encoders.bean(A.class);
        final ConformTransform conform = ConformTransform.builder().structType(encoder.schema()).build();

        sourceA.then(schema).then(conform).then(dataset -> {

            final List<A> rows = dataset.as(Encoders.bean(A.class)).collectAsList();
            assertEquals(1, rows.size());
            assertTrue(rows.contains(new A("a:1", new B("b:1", null, null))));
        });
    }
}

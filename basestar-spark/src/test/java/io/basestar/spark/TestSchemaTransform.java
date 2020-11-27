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
import io.basestar.spark.sink.Sink;
import io.basestar.spark.source.Source;
import io.basestar.spark.transform.ConformTransform;
import io.basestar.spark.transform.MarshallTransform;
import io.basestar.spark.transform.SchemaTransform;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSchemaTransform extends AbstractSparkTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class B2 {

        private String value;
    }

    @Test
    void testSchemaTransform() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(TestSchemaTransform.class.getResourceAsStream("schema.yml"));

        final ObjectSchema a = namespace.requireObjectSchema("A");

        final Source<Dataset<Row>> sourceA = (Source<Dataset<Row>>) sink -> sink.accept(session.createDataset(ImmutableList.of(
                new A("a:1", new B("b:1", null, 0L), null, null, null)
        ), Encoders.bean(A.class)).toDF());

        final SchemaTransform schema = SchemaTransform.builder().schema(a).build();

        final Encoder<A> encoder = Encoders.bean(A.class);
        final ConformTransform conform = ConformTransform.builder().structType(encoder.schema()).build();

        sourceA.then(schema).then(conform).then((Sink<Dataset<Row>>) (dataset -> {

            final List<A> rows = dataset.as(Encoders.bean(A.class)).collectAsList();
            assertEquals(1, rows.size());
            assertTrue(rows.contains(new A("a:1", new B("b:1", null, null), null, null, null)));
        }));
    }

    @Test
    void testSchemaTransformInvalid2() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final ObjectSchema b = namespace.requireObjectSchema("B");

        final Source<Dataset<Row>> sourceA = (Source<Dataset<Row>>) sink -> sink.accept(session.createDataset(ImmutableList.of(
                new B2("1"), new B2("x")
        ), Encoders.bean(B2.class)).toDF());

        final SchemaTransform schema = SchemaTransform.builder().schema(b).build();

        final Encoder<B> encoder = Encoders.bean(B.class);
        final ConformTransform conform = ConformTransform.builder().structType(encoder.schema()).build();

        sourceA.then(schema).then(conform).then(dataset -> {

            final List<B> rows = dataset.as(Encoders.bean(B.class)).collectAsList();
            assertEquals(2, rows.size());
            assertTrue(rows.contains(new B(null, null, 1L)));
            assertTrue(rows.contains(new B(null, null, null)));
        });
    }

    @Test
    void testSchemaTransformInvalid() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final ObjectSchema b = namespace.requireObjectSchema("B");

        final Source<Dataset<Row>> sourceA = (Source<Dataset<Row>>) sink -> sink.accept(session.createDataset(ImmutableList.of(
                new B2("1"), new B2("x")
        ), Encoders.bean(B2.class)).toDF());

        final MarshallTransform<B> marshall = MarshallTransform.<B>builder().targetType(B.class).build();

        sourceA.then(marshall).then(dataset -> {

            final List<B> rows = dataset.collectAsList();
            assertEquals(2, rows.size());
            assertTrue(rows.contains(new B(null, null, 1L)));
            assertTrue(rows.contains(new B(null, null, null)));
        });
    }

    @Test
    void testDateTimes() throws IOException {

        final SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        final Namespace namespace = Namespace.load(TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final ObjectSchema f = namespace.requireObjectSchema("F");

        final Encoder<F> encoder = Encoders.bean(F.class);

        final java.sql.Date date = new java.sql.Date(2020 - 1900, 10, 10);
        final java.sql.Timestamp datetime = new java.sql.Timestamp(Instant.now().toEpochMilli());

        final Source<Dataset<Row>> sourceF = sink -> sink.accept(session.createDataset(ImmutableList.of(
                new F(date, datetime)
        ), encoder).toDF());

        final SchemaTransform schema = SchemaTransform.builder().schema(f).build();

        final ConformTransform conform = ConformTransform.builder().structType(encoder.schema()).build();

        sourceF.then(schema).then(conform).then(dataset -> {

            final List<F> rows = dataset.as(Encoders.bean(F.class)).collectAsList();
            assertEquals(1, rows.size());
            assertTrue(rows.contains(new F(date, datetime)));
        });
    }
}

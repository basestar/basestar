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
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.resolver.SchemaResolver;
import io.basestar.spark.transform.ConformTransform;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExpandTransform extends AbstractSparkTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExpandedA {

        private String id;

        private B ref;

        private List<B> arrayRef;

        private Map<String, B> mapRef;

        private E structRef;

        private List<CollapsedC> multi;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CollapsedC {

        private String id;
    }

    @Test
    void testExpandTransform() throws IOException {

        final SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        final Namespace namespace = Namespace.load(TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final ObjectSchema a = namespace.requireObjectSchema("A");
        final ObjectSchema b = namespace.requireObjectSchema("B");
        final ObjectSchema c = namespace.requireObjectSchema("C");

        final B b1 = new B("b:1", new D("d:10"), 10L);
        final B b2 = new B("b:2", new D("d:20"), 100L);
        final B b3 = new B("b:3", new D("d:30"), 1000L);
        final B b4 = new B("b:4", new D("d:40"), 10000L);
        final B b5 = new B("b:5", new D("d:50"), 100000L);

        final Dataset<Row> datasetA = session.createDataset(ImmutableList.of(
                new A("a:1", new B("b:1"), ImmutableList.of(new B("b:3"), new B("b:4")), ImmutableMap.of("x", new B("b:5")), new E(new B("b:1"))),
                new A("a:2", new B("b:2"), ImmutableList.of(), ImmutableMap.of(), null),
                new A("a:3", null, ImmutableList.of(), ImmutableMap.of(), null),
                new A("a:4", null, ImmutableList.of(), ImmutableMap.of(), null),
                new A("a:5", new B("b:2"), ImmutableList.of(), ImmutableMap.of(), null)
        ), Encoders.bean(A.class)).toDF();

        final Dataset<Row> datasetB = session.createDataset(ImmutableList.of(
                b1, b2, b3, b4, b5
        ), Encoders.bean(B.class)).toDF();

        final Dataset<Row> datasetC = session.createDataset(ImmutableList.of(
                new C("c:1", new A("a:1")),
                new C("c:2", new A("a:1")),
                new C("c:3", new A("a:2"))
        ), Encoders.bean(C.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                a.getQualifiedName(), datasetA,
                b.getQualifiedName(), datasetB,
                c.getQualifiedName(), datasetC
        );

        final Set<Name> expand = Name.parseSet("ref,single,multi,structRef.ref,arrayRef,mapRef.*");

        final SchemaResolver resolver = new SchemaResolver.Automatic((schema, exp) -> datasets.get(schema.getQualifiedName()));

        final Dataset<Row> dataset = resolver.resolve(a, expand);

        final Encoder<ExpandedA> encoder = Encoders.bean(ExpandedA.class);
        final ConformTransform conform = ConformTransform.builder().structType(encoder.schema()).build();

        final List<ExpandedA> rows = conform.accept(dataset).as(encoder).collectAsList();
        assertEquals(5, rows.size());
        assertTrue(rows.contains(new ExpandedA("a:1", b1, ImmutableList.of(b3, b4), ImmutableMap.of("x", b5), new E(b1), ImmutableList.of(
                new CollapsedC("c:1"),
                new CollapsedC("c:2")
        ))));
        assertTrue(rows.contains(new ExpandedA("a:2", b2, ImmutableList.of(), ImmutableMap.of(), null, ImmutableList.of(
                new CollapsedC("c:3")
        ))));
        assertTrue(rows.contains(new ExpandedA("a:3", null, ImmutableList.of(), ImmutableMap.of(), null, ImmutableList.of())));
        assertTrue(rows.contains(new ExpandedA("a:4", null, ImmutableList.of(), ImmutableMap.of(), null, ImmutableList.of())));
        assertTrue(rows.contains(new ExpandedA("a:5", b2, ImmutableList.of(), ImmutableMap.of(), null, ImmutableList.of())));
    }
}

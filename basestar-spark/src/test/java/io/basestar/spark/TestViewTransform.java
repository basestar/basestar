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
import io.basestar.schema.ViewSchema;
import io.basestar.spark.transform.ConformTransform;
import io.basestar.spark.util.ColumnResolver;
import io.basestar.spark.util.DatasetResolver;
import io.basestar.util.Name;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestViewTransform extends AbstractSparkTest {

    @Test
    public void testViewTransform() throws IOException {

        final SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        final Namespace namespace = Namespace.load(TestViewTransform.class.getResourceAsStream("schema.yml"));

        final D d1 = new D("a");
        final D d2 = new D("b");

        final ObjectSchema b = namespace.requireObjectSchema("B");
        final ViewSchema agg = namespace.requireViewSchema("AggView");

        final Dataset<Row> datasetB = session.createDataset(ImmutableList.of(
                new B("1", d1, 1L), new B("2", d1, 2L), new B("3", d1, 3L),
                new B("4", d2, 2L), new B("5", d2, 4L), new B("6", d2, 6L)
        ), Encoders.bean(B.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                b.getQualifiedName(), datasetB
        );

        final DatasetResolver resolver = DatasetResolver.automatic((schema) -> datasets.get(schema.getQualifiedName()));

        final Dataset<Row> dataset = resolver.resolve(agg, ColumnResolver::nested);

        final Encoder<AggView> encoder = Encoders.bean(AggView.class);
        final ConformTransform conform = ConformTransform.builder().structType(encoder.schema()).build();

        final List<AggView> rows = conform.accept(dataset).as(encoder).collectAsList();
        assertEquals(2, rows.size());
        assertTrue(rows.contains(new AggView("a", 8)));
        assertTrue(rows.contains(new AggView("b", 14)));
    }
}

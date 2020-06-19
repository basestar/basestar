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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestViewTransform {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class A {

        private String id;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class B {

        private A key;

        private int value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggView {

        private String key;

        private long agg;
    }

    @Test
    public void testView() throws IOException {

        final SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        final Namespace namespace = Namespace.load(TestViewTransform.class.getResourceAsStream("schema.yml"));

        final A a = new A("a");
        final A b = new A("b");

        final Source<Dataset<Row>> sourceB = (Source<Dataset<Row>>) sink -> sink.accept(session.createDataset(ImmutableList.of(
                new B(a, 1), new B(a, 2), new B(a, 3), new B(b, 2), new B(b, 4), new B(b, 6)
        ), Encoders.bean(B.class)).toDF());

        final ViewTransform view = ViewTransform.builder()
                .schema(namespace.requireViewSchema("AggView"))
                .build();

        sourceB.then(view).then(dataset -> {

            final List<AggView> rows = dataset.as(Encoders.bean(AggView.class)).collectAsList();
            assertEquals(2, rows.size());
            assertTrue(rows.contains(new AggView("a", 8)));
            assertTrue(rows.contains(new AggView("b", 14)));
        });
    }
}

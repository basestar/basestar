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

public class TestViewTransform {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
//    @Accessors(chain = true)
    public static class B {

        private String key;

        private int value;
    }

    @Test
    public void testView() throws IOException {

        final SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        final Namespace namespace = Namespace.load(TestViewTransform.class.getResourceAsStream("schema.yml"));

        final Source<Dataset<Row>> sourceB = (Source<Dataset<Row>>) sink -> sink.accept(session.createDataset(ImmutableList.of(
                new B("a", 1), new B("a", 2), new B("a", 3), new B("b", 2), new B("b", 4), new B("b", 6)
        ), Encoders.bean(B.class)).toDF());

        final ViewTransform view = ViewTransform.builder()
                .schema(namespace.requireViewSchema("AggView"))
                .build();

        sourceB.then(view).then(dataset -> {

            System.err.println(dataset.collectAsList());
        });
    }
}

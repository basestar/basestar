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
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.Path;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestExpandTransform {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
//    @Accessors(chain = true)
    public static class A {

        private String id;

        private B ref;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
//    @Accessors(chain = true)
    public static class B {

        private String id;
    }

    @Data
//    @Accessors(chain = true)
    public static class C {

    }

    @Test
    public void testExpand() throws IOException {

        final SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        final Namespace namespace = Namespace.load(TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final ObjectSchema a = namespace.requireObjectSchema("A");

        final Source<Dataset<Row>> sourceA = (Source<Dataset<Row>>) sink -> sink.accept(session.createDataset(ImmutableList.of(
                new A("a:1", new B("b:1"))
        ), Encoders.bean(A.class)).toDF());

        final ExpandTransform expand = ExpandTransform.builder()
                .schema(a)
                .expand(ImmutableSet.of(
                        Path.of("ref"),
                        Path.of("link")
                ))
                .sources((name) -> {
                    return null;
                })
                .build();

        final SchemaTransform schema = SchemaTransform.builder().schema(a).build();

        sourceA.then(schema).then(dataset -> {

            System.err.println(dataset.collectAsList());
        });
    }
}

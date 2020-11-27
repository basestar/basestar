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

import io.basestar.mapper.annotation.ObjectSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public abstract class AbstractSparkTest {

    protected SparkSession session() {

        return SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class A {

        private String id;

        private B ref;

        private List<B> arrayRef;

        private Map<String, B> mapRef;

        private E structRef;

        public A(final String id) {

            this(id, null);
        }

        public A(final String id, final B ref) {

            this(id, ref, null, null, null);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StatsA {

        private String refId;

        private Long count;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class B {

        private String id;

        private D key;

        private Long value;

        private D key2;

        public B(final String id) {

            this(id, null, null);
        }

        public B(final String id, final D key, final Long value) {

            this(id, key, value, null);
        }

        public B withoutKeys() {

            return new B(id, key == null ? null : new D(key.getId()), value, key2 == null ? null : new D(key2.getId()));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BFlat {

        private String id;

        private String key;

        private Long value;

        private String key2;

        public BFlat(final String id) {

            this(id, null, null);
        }

        public BFlat(final String id, final String key, final Long value) {

            this(id, key, value, null);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class C {

        private String id;

        private A owner;

        public C(final String id) {

            this(id, null);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class D {

        private String id;

        private Long x;

        public D(final String id) {

            this(id, null);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class E {

        private B ref;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class F {

        private java.sql.Date date;

        private java.sql.Timestamp datetime;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ObjectSchema(name = "F")
    public static class F2 {

        private LocalDate date;

        private Instant datetime;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggView {

        private String key;

        private double agg;

        private List<B> collect;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AliasView {

        private String id;

        private Long count;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LinkingView {

        private String id;

        private B record;

        private D key;

        private D key2;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class File {

        private String id;

        private Map<String, String> tags;

        public File(final String id) {

            this(id, null);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileRow {

        private String id;

        private File file;

        private Long rowIndex;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HeaderRows {

        private String id;

        private List<FileRow> rows;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LinkingViewToView {

        private String id;

        private HeaderRows headerRows;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Expressions {

        private String substr1;

        private String substr2;

        private String mapValue;
    }
}

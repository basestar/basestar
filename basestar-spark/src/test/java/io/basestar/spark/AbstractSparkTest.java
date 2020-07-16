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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

public abstract class AbstractSparkTest {

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
    public static class B {

        private String id;

        private D key;

        private Long value;

        public B(final String id) {

            this(id, null, null);
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
    public static class AggView {

        private String key;

        private long agg;
    }
}

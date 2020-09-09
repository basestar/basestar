package io.basestar.mapper;

/*-
 * #%L
 * basestar-mapper
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

import io.basestar.type.PropertyContext;
import io.basestar.type.TypeContext;
import io.basestar.type.has.HasType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFullType {

    interface B<F> {

        default F getF(final F a) {

            return a;
        }

        default F getF() {

            return null;
        }
    }

    public static class A implements B<Integer> {

        Integer b;

        void setF(final Integer a) {

        }

        void setAWSTest(final Integer a) {

        }

        void getTest() {

        }
    }

    @Test
    public void testSimple() {

        final TypeContext a = TypeContext.from(A.class);
        final List<PropertyContext> props = a.properties().stream()
                .filter(HasType.match(Integer.class))
                .collect(Collectors.toList());
        assertEquals(3, props.size());
    }
}

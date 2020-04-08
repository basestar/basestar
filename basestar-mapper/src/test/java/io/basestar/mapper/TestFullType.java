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

import org.junit.jupiter.api.Test;

public class TestFullType {

    interface B<F> {

        default F getF(F a) {

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

        @Test
        void setAWSTest(final Integer a) {

        }

        void getTest() {

        }
    }

    @Test
    public void testSimple() {

//        final WithType<A> a = WithType.with(A.class);
//        a.methods().iterator().next().type().methods().iterator().next().



//        final WithMethod<A, ?> method = a.declaredMethods().stream()
//                .filter(HasAnnotations.match(Test.class))
//        .findFirst().orElse(null);
//        System.err.println(method);



//        reifiedType.typeParameters().get(0).name();
//


//        final Bean.Method<? super A, ?> m = bean.method(Bean.Method.ha("getF", Integer.class));


//                .and(Bean.Modified.hasModifier(Modifier.PUBLIC))
//                .and(Bean.Named.hasName("get")));

//        System.err.println(a);
    }
}

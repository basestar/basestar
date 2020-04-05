package io.basestar.mapper;

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

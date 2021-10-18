package io.basestar.type;

import com.google.common.collect.ImmutableMap;
import io.basestar.type.has.*;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unused")
class TestTypeContext {

    private static class Base {

        @Nonnull
        public String field = "";
    }

    private interface IFace<T> {

        T getValue();
    }

    private static class Derived extends Base implements IFace<Integer> {

        public Derived() {
        }

        public Derived(final String field) {

            this.field = field;
        }

        @Override
        public Integer getValue() {

            return 5;
        }

        @Override
        public String toString() {

            return "derived:" + field;
        }
    }

    @Test
    void testTypeContext() throws Exception {

        final TypeContext derived = TypeContext.from(Derived.class);

        assertEquals(Base.class, derived.superclass().erasedType());

        final List<TypeContext> interfaces = derived.interfaces();
        assertEquals(1, interfaces.size());
        assertEquals(IFace.class, interfaces.get(0).erasedType());
        assertEquals(Integer.class, interfaces.get(0).typeParameters().get(0).erasedType());

        final List<ConstructorContext> ctors = derived.constructors();
        assertEquals(2, ctors.size());

        final List<PropertyContext> properties = derived.properties();
        assertEquals(2, properties.size());

        final PropertyContext value = derived.properties().stream().filter(HasName.match("value")).findFirst()
                .orElseThrow(IllegalStateException::new);

        final PropertyContext prop = derived.properties().stream().filter(HasAnnotations.match(Nonnull.class)).findFirst()
                .orElseThrow(IllegalStateException::new);

        final FieldContext field = derived.fields().stream().filter(HasModifiers::isPublic)
                .filter(HasName.match("field"))
                .filter(HasType.match(String.class))
                .findFirst().orElseThrow(IllegalStateException::new);

        final Derived d1 = new Derived();
        final SerializableAccessor propAccessor = prop.serializableAccessor();
        propAccessor.set(d1, "test");
        assertEquals("test", propAccessor.get(d1));
        prop.set(d1, "test2");
        assertEquals("test2", prop.get(d1));
        final SerializableAccessor fieldAccessor = field.serializableAccessor();
        fieldAccessor.set(d1, "test3");
        assertEquals("test3", fieldAccessor.get(d1));
        field.set(d1, "test4");
        assertEquals("test4", field.get(d1));
        assertEquals((Integer) 5, value.get(d1));

        final MethodContext toString = derived.method("toString").orElseThrow(IllegalStateException::new);
        assertEquals("derived:test4", toString.invoke(d1));
        assertEquals("derived:test4", toString.serializableInvoker().invoke(d1));

        final ConstructorContext ctor = ctors.stream().filter(HasParameters.match(String.class)).findFirst().orElseThrow(IllegalStateException::new);
        final Derived d2 = ctor.newInstance("test5");
        assertEquals("test5", d2.field);
    }

    @Test
    void testAnnotationContext() {

        final AnnotationContext<SuppressWarnings> context = new AnnotationContext<>(SuppressWarnings.class, ImmutableMap.of(
                "value", new String[]{"unchecked"}
        ));
        assertArrayEquals(new String[]{"unchecked"}, context.value());
        final SuppressWarnings annot = context.annotation();
        assertNotNull(annot);
        assertEquals(1, annot.value().length);
        assertEquals("unchecked", annot.value()[0]);
        final AnnotationContext<SuppressWarnings> otherContext = new AnnotationContext<>(annot);
        assertArrayEquals(new String[]{"unchecked"}, otherContext.value());
        assertArrayEquals(new String[]{"unchecked"}, (String[]) otherContext.nonDefaultValues().get("value"));
    }
}

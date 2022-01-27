package io.basestar.schema;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSchema {

    @Test
    void testRebuild() throws Exception {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final Namespace.Builder builder = Namespace.builder()
                .setSchemas(Immutable.transformValues(namespace.getSchemas(), (k, v) -> v.descriptor()));

        builder.build();
    }

    @Test
    void testMaterializationDependencies() throws Exception {

        final Namespace namespace = Namespace.load(TestSchema.class.getResource("dependencies.yml"));

        final ViewSchema view = namespace.requireViewSchema("DerivedView");

        final Set<Name> deps = view.materializationDependencies(view.getExpand()).keySet();

        assertEquals(ImmutableSet.of(Name.of("BaseView"), Name.of("Object1"), Name.of("Object2"),
                Name.of("Linked"), Name.of("Ref1"), Name.of("Ref2"), Name.of("Ref3")), deps);
    }

    @Test
    void testQueries() throws Exception {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Order");
        assertEquals(3, schema.getQueries().size());
        final Query query = schema.requireQuery("byPet", true);
        assertEquals(Expression.parseAndBind(Context.init(), "pet.id == petId"), query.getExpression());
        assertEquals(1, query.getArguments().size());
    }

    @Test
    void testUnknownSchemaType() {

        assertThrows(SchemaValidationException.class, () -> {
            try {
                Namespace.load(TestInterfaceSchema.class.getResource("unknown.yml"));
            } catch (final JsonMappingException e) {
                if (e.getCause() instanceof SchemaValidationException) {
                    throw e.getCause();
                } else {
                    throw e;
                }
            }
        });
    }
}

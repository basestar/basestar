package io.basestar.codegen;

import com.google.common.collect.ImmutableMap;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.MappingStrategy;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.TypeMapper;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Member;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.schema.use.Use;
import io.basestar.type.PropertyContext;
import io.basestar.type.TypeContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Path;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TestTypescriptCodegen {

    Namespace namespace() throws IOException {

        return Namespace.load(TestJavaCodegen.class.getResource("schema.yml"));
    }

    @Test
    @Disabled
    void testAdvancedCodegen() throws IOException {

        final MappingContext mappingContext = new MappingContext(new MappingStrategy() {

            @Override
            public Name schemaName(final MappingContext context, final TypeContext type) {

                final String simpleName = type.simpleName();
                if(Use.class.isAssignableFrom(type.erasedType())) {
                    return Name.of("use", simpleName);
                } else if ("Descriptor".equals(type.simpleName())) {
                    return Name.of(type.enclosing().simpleName());
                } else {
                    return Name.of(simpleName);
                }
            }

            @Override
            public boolean concrete(final TypeContext type) {

                final Class<?> erased = type.erasedType();
                return !(erased.equals(Schema.Descriptor.class) || erased.equals(InstanceSchema.Descriptor.class)
                        || erased.equals(Member.Descriptor.class) || erased.equals(Use.class));
            }

            @Override
            public List<TypeContext> extend(final TypeContext type) {

                // These are interfaces, so have to explicitly set them
                final Class<?> erased = type.erasedType();
                if(erased.equals(io.basestar.schema.InstanceSchema.Descriptor.class)) {
                    return Immutable.list(TypeContext.from(Schema.Descriptor.class));
                } else if(erased.equals(io.basestar.schema.EnumSchema.Descriptor.class)) {
                    return Immutable.list(TypeContext.from(Schema.Descriptor.class));
                } else if(erased.equals(io.basestar.schema.ObjectSchema.Descriptor.class)
                        || erased.equals(io.basestar.schema.StructSchema.Descriptor.class)
                        || erased.equals(io.basestar.schema.ViewSchema.Descriptor.class)) {
                    return Immutable.list(TypeContext.from(InstanceSchema.Descriptor.class));
                } else if(Member.Descriptor.class.isAssignableFrom(erased) && !Member.Descriptor.class.equals(erased)) {
                    return Immutable.list(TypeContext.from(Member.Descriptor.class));
                } else if(Use.class.isAssignableFrom(erased) && !Use.class.equals(erased)) {
                    return Immutable.list(TypeContext.from(Use.class));
                }
                return Immutable.list();
            }

            @Override
            public TypeMapper typeMapper(final MappingContext context, final TypeContext type) {

                return TypeMapper.fromDefault(context, type);
            }

            @Override
            public boolean isOptional(final PropertyContext property) {

                return true;
            }
        });

        final Class<?>[] classes = {
                io.basestar.schema.Schema.Descriptor.class,
                io.basestar.schema.InstanceSchema.Descriptor.class,
                io.basestar.schema.ObjectSchema.Descriptor.class,
                io.basestar.schema.StructSchema.Descriptor.class,
                io.basestar.schema.EnumSchema.Descriptor.class,
                io.basestar.schema.ViewSchema.Descriptor.class,
                io.basestar.schema.Member.Descriptor.class,
                io.basestar.schema.Property.Descriptor.class,
                io.basestar.schema.Link.Descriptor.class,
                io.basestar.schema.Permission.Descriptor.class,
                io.basestar.schema.History.class,
                io.basestar.schema.Consistency.class,
                io.basestar.schema.Concurrency.class,
                io.basestar.schema.Constraint.class,
                io.basestar.schema.ViewSchema.Descriptor.From.class,
                io.basestar.schema.validation.Validation.Validator.class,
                io.basestar.schema.Transient.Descriptor.class,
                io.basestar.schema.Visibility.class,
                io.basestar.schema.Id.Descriptor.class,
                io.basestar.schema.Index.Descriptor.class,
                io.basestar.schema.use.Use.class,
                io.basestar.schema.use.UseAny.class,
                io.basestar.schema.use.UseArray.class,
                io.basestar.schema.use.UseBoolean.class,
                io.basestar.schema.use.UseBinary.class,
                io.basestar.schema.use.UseDate.class,
                io.basestar.schema.use.UseDateTime.class,
                io.basestar.schema.use.UseInteger.class,
                io.basestar.schema.use.UseMap.class,
                io.basestar.schema.use.UseNumber.class,
                io.basestar.schema.use.UseSet.class,
                io.basestar.schema.use.UseString.class,
                io.basestar.schema.use.UseNamed.class,
                io.basestar.schema.use.UseOptional.class
        };

        final Map<Name, Schema.Descriptor<?, ?>> schemas = Arrays.stream(classes)
                .map(mappingContext::schemaMapper).collect(Collectors.toMap(
                        SchemaMapper::qualifiedName,
                        SchemaMapper::schemaBuilder
                ));

        final Namespace namespace = Namespace.builder().setSchemas(schemas).build();

        final CodegenSettings settings = CodegenSettings.builder()
                .packageName("io.basestar.test")
                .codebehind(ImmutableMap.of(
                        Name.parse("use.Use"), Codebehind.builder()
                                .name(Name.parse("Use"))
                                .build(),
                        Name.parse("Schema"), Codebehind.builder()
                                .name(Name.parse("Schema"))
                                .generate(true)
                                .build(),
                        Name.parse("Visibility"), Codebehind.builder()
                                .name(Name.parse("Visibility"))
                                .build(),
                        Name.parse("Constraint"), Codebehind.builder()
                                .name(Name.parse("Constraint"))
                                .build()
                ))
                .codebehindPath(Path.parse("../src"))
                .build();
        final Codegen codegen = new Codegen("typescript", settings);

        final File codegenDirectory = new File("/Users/mattevans/basestar/basestar-new/basestar-js/generated-src");
        codegen.generate(namespace, codegenDirectory);
    }
}

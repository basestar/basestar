package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableList;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.MappingStrategy;
import io.basestar.mapper.SchemaMapper;
import io.basestar.mapper.internal.ViewSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaDeclaration;
import io.basestar.schema.from.From;
import io.basestar.schema.util.SchemaRef;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SchemaDeclaration(SqlViewSchema.Declaration.class)
public @interface SqlViewSchema {

    String name() default MappingStrategy.INFER_NAME;

    boolean materialized() default false;

    String query();

    String[] primaryKey();

    Using[] using();

    @interface Using {

        String as();

        String schema();
    }

    @RequiredArgsConstructor
    class Declaration implements SchemaDeclaration.Declaration {

        private final SqlViewSchema annotation;

        @Override
        public Name getQualifiedName(final MappingContext context, final TypeContext type) {

            return context.strategy().schemaName(context, annotation.name(), type);
        }

        @Override
        public SchemaMapper.Builder<?, ?> mapper(final MappingContext context, final TypeContext type) {

            return ViewSchemaMapper.builder(context, getQualifiedName(context, type), type)
                    .setMaterialized(annotation.materialized())
                    .setSql(annotation.query())
                    .setPrimaryKey(ImmutableList.copyOf(annotation.primaryKey()))
                    .setUsing(Stream.of(annotation.using()).collect(Collectors.toMap(
                            Using::as,
                            e -> From.builder().setSchema(SchemaRef.withName(Name.parse(e.schema())))
                    )));
        }

        public static SqlViewSchema annotation(final io.basestar.schema.ViewSchema schema) {

            throw new UnsupportedOperationException();
//            final FromSql from = (FromSql)schema.getFrom();
//            return new AnnotationContext<>(SqlViewSchema.class, ImmutableMap.<String, Object>builder()
//                    .put("name", schema.getQualifiedName().toString())
//                    .put("materialized", schema.isMaterialized())
//                    .put("query", from.getSql())
//                    .put("primaryKey", from.getPrimaryKey().toArray(new String[0]))
//                    .put("using", from.getUsing().entrySet().stream().map(
//                            e -> new AnnotationContext<>(Using.class, ImmutableMap.<String, Object>builder()
//                                    .put("as", e.getKey())
//                                    .put("schema", ((FromSchema)e.getValue()).getSchema().getQualifiedName().toString())
//                                    .build()).annotation()
//                    ).toArray(Using[]::new))
//                    .build()).annotation();

        }
    }
}

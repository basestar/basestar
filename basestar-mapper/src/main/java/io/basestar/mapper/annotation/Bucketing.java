package io.basestar.mapper.annotation;

import com.google.common.collect.ImmutableMap;
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.internal.LinkableSchemaMapper;
import io.basestar.mapper.internal.annotation.SchemaModifier;
import io.basestar.type.AnnotationContext;
import io.basestar.util.AbstractPath;
import io.basestar.util.BucketFunction;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@SchemaModifier(Bucketing.Modifier.class)
public @interface Bucketing {

    Bucket[] value();

    @interface Bucket {

        String[] using();

        int count() default io.basestar.schema.Bucketing.DEFAULT_COUNT;

        BucketFunction function() default BucketFunction.MURMER3_32;
    }

    @RequiredArgsConstructor
    class Modifier implements SchemaModifier.Modifier<LinkableSchemaMapper.Builder<?, ?>> {

        private final Bucketing annotation;

        public static Bucketing annotation(final List<io.basestar.schema.Bucketing> bucketing) {

            final Bucket[] buckets = bucketing.stream().map(v -> {

                final String[] using = v.getUsing().stream().map(AbstractPath::toString).toArray(String[]::new);

                return new AnnotationContext<>(Bucket.class, ImmutableMap.<String, Object>builder()
                        .put("using", using)
                        .put("count", v.getCount())
                        .put("function", v.getFunction())
                        .build()).annotation();

            }).toArray(Bucket[]::new);

            return new AnnotationContext<>(Bucketing.class, ImmutableMap.<String, Object>builder()
                    .put("value", buckets)
                    .build()).annotation();
        }

        @Override
        public void modify(final MappingContext context, final LinkableSchemaMapper.Builder<?, ?> mapper) {

            mapper.setBucketing(Arrays.stream(annotation.value()).map(v -> {

                final List<Name> using = Arrays.stream(v.using()).map(Name::parse).collect(Collectors.toList());
                return new io.basestar.schema.Bucketing(using, v.count(), v.function());

            }).collect(Collectors.toList()));
        }
    }
}

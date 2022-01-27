package io.basestar.schema;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.util.Immutable;
import io.basestar.util.Text;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface SchemaClasspath {

    SchemaClasspath DEFAULT = new SchemaClasspath.Default();

    Class<? extends Schema.Builder<?, ?, ?>> classForId(String id);

    String idForClass(Class<? extends Schema.Builder<?, ?, ?>> cls);

    class Default implements SchemaClasspath {

        public static final String PROPERTY = "io.basestar.schema.classpath";

        private static final Pattern INPUT_NAME_REGEX = Pattern.compile("[a-z\\-]+");

        private static final Pattern OUTPUT_NAME_REGEX = Pattern.compile("[A-Za-z.]+\\.([A-Za-z]+)Schema\\$Builder");

        private static final List<String> DEFAULT_SEARCH_PACKAGES = Immutable.list(
                "io.basestar.schema"
        );

        private final Map<String, Class<? extends Schema.Builder<?, ?, ?>>> classes = new ConcurrentHashMap<>();

        private Stream<String> searchPackages() {

            return Stream.concat(
                    DEFAULT_SEARCH_PACKAGES.stream(),
                    Optional.ofNullable(System.getProperty(PROPERTY))
                            .map(v -> Splitter.on(",").omitEmptyStrings().trimResults().splitToList(v))
                            .orElse(ImmutableList.of()).stream()
            );
        }

        private Class<? extends Schema.Builder<?, ?, ?>> loadClassForId(final String id) {

            final List<Class<?>> matched;
            if (INPUT_NAME_REGEX.matcher(id).matches()) {
                final String className = Text.upperCamel(id) + "Schema$Builder";
                matched = searchPackages().flatMap(packageName -> {
                    final String qualifiedName = packageName + "." + className;
                    try {
                        final Class<?> cls = Class.forName(qualifiedName);
                        if (Schema.Builder.class.isAssignableFrom(cls)) {
                            return Stream.of(cls);
                        }
                    } catch (final ClassNotFoundException e) {
                        // Skip
                    }
                    return Stream.empty();
                }).collect(Collectors.toList());
            } else {
                matched = ImmutableList.of();
            }
            if (matched.size() == 1) {
                @SuppressWarnings("unchecked") final Class<? extends Schema.Builder<?, ?, ?>> cls = (Class<? extends Schema.Builder<?, ?, ?>>) matched.get(0);
                return cls;
            } else if (matched.size() == 0) {
                throw new SchemaValidationException("Schema implementation for " + id + " not found");
            } else {
                throw new SchemaValidationException("Schema implementation for " + id + " is ambiguous " + matched + " match");
            }
        }

        @Override
        public Class<? extends Schema.Builder<?, ?, ?>> classForId(final String id) {

            return classes.computeIfAbsent(id, this::loadClassForId);
        }

        @Override
        public String idForClass(final Class<? extends Schema.Builder<?, ?, ?>> cls) {

            final String name = cls.getName();
            final Matcher matcher = OUTPUT_NAME_REGEX.matcher(name);
            if (matcher.matches()) {
                final String match = matcher.group(1);
                return Text.lowerCamel(match);
            } else {
                throw new SchemaValidationException("Schema name " + name + " not valid");
            }
        }
    }
}

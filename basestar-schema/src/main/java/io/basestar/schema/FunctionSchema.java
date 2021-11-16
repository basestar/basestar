package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.expression.Context;
import io.basestar.expression.call.Callable;
import io.basestar.schema.from.From;
import io.basestar.schema.from.FromSchema;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Accessors(chain = true)
public class FunctionSchema implements Schema<Callable> {

    public static final Pattern DEFINITION_REPLACEMENT_REGEX = Pattern.compile("\"@\\{(.*?)}\"|@\\{(.*?)}");

    @Nonnull
    private final Name qualifiedName;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    /**
     * Description of the schema
     */

    @Nullable
    private final String description;

    @Nonnull
    private final String language;

    @Nonnull
    private final List<Argument> arguments;

    @Nonnull
    private final Use<?> returns;

    @Nonnull
    private final String definition;

    @Nonnull
    private final Map<String, Serializable> extensions;

    @Nonnull
    private final Map<String, From> using;

    public FunctionSchema(final Descriptor descriptor, final Resolver.Constructing resolver, final Version version, @Nonnull final Name qualifiedName, final int slot) {

        resolver.constructing(qualifiedName, this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.description = descriptor.getDescription();
        this.language = Nullsafe.require(descriptor.getLanguage());
        this.arguments = Immutable.transform(descriptor.getArguments(), v -> v.build(resolver));
        this.returns = Nullsafe.require(descriptor.getReturns()).resolve(resolver);
        this.definition = Nullsafe.require(descriptor.getDefinition());
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        this.using = Immutable.transformValues(descriptor.getUsing(), (k, v) -> v.build(resolver, Context.init()));
    }

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Schema.Descriptor<FunctionSchema, Callable> {

        String TYPE = "function";

        @Override
        default String getType() {

            return TYPE;
        }

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        String getLanguage();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Argument.Descriptor> getArguments();

        Use<?> getReturns();

        String getDefinition();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, From.Descriptor> getUsing();

        interface Self extends Schema.Descriptor.Self<FunctionSchema, Callable>, FunctionSchema.Descriptor {

            @Override
            default String getLanguage() {

                return self().getLanguage();
            }

            default List<Argument.Descriptor> getArguments() {

                return Immutable.transform(self().getArguments(), Argument::descriptor);
            }

            @Override
            default Use<?> getReturns() {

                return self().getReturns();
            }

            @Override
            default String getDefinition() {

                return self().getDefinition();
            }

            default Map<String, From.Descriptor> getUsing() {

                return Immutable.transformValues(self().getUsing(), (k, v) -> v.descriptor());
            }
        }

        @Override
        default FunctionSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new FunctionSchema(this, resolver, version, qualifiedName, slot);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "language", "arguments", "returns", "definition", "extensions"})
    public static class Builder implements Schema.Builder<Builder, FunctionSchema, Callable>, FunctionSchema.Descriptor {

        @Nullable
        private Long version;

        @Nullable
        private String description;

        @Nullable
        private String language;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<Argument.Descriptor> arguments;

        @Nullable
        private Use<?> returns;

        @Nullable
        private String definition;

        @Nullable
        private Map<String, Serializable> extensions;

        @Nullable
        private Map<String, From.Descriptor> using;
    }

    @Override
    public Callable create(final ValueContext context, final Object value, final Set<Name> expand) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Callable after) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Type javaType(final Name name) {

        throw new UnsupportedOperationException();
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return null;
    }

    @Override
    public FunctionSchema.Descriptor descriptor() {

        return (Descriptor.Self) () -> FunctionSchema.this;
    }

    @Override
    public Use<Callable> typeOf() {

        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(final Callable value) {

        throw new UnsupportedOperationException();
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        if (!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            using.forEach((k, v) -> v.collectDependencies(out));
        }
    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, LinkableSchema> out) {

        using.forEach((k, v) -> v.collectMaterializationDependencies(out));
    }

    public String getReplacedDefinition(final Function<Schema<?>, String> replacer) {

        return getReplacedDefinition(definition, using, replacer);
    }

    public static String replaceConcrete(final String definition, final Map<String, From> using,
                                         final Function<Schema<?>, String> replacer) {

        final Matcher matcher = DEFINITION_REPLACEMENT_REGEX.matcher(definition);
        final StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            final String name = Nullsafe.orDefault(matcher.group(1), matcher.group(2));
            final From use = using.get(name);
            if (use instanceof FromSchema) {
                matcher.appendReplacement(buffer, replacer.apply(((FromSchema) use).getSchema()));
            } else {
                throw new IllegalStateException("SQL view schema does not declare " + name);
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }


    public static String getReplacedDefinition(final String definition, final Map<String, From> using,
                                               final Function<Schema<?>, String> replacer) {

        if (using.keySet().size() == 0) {
            return definition;
        }
        final String groups = using.keySet().stream()
                .flatMap(name -> Stream.of(name, "\"" + name + "\""))
                .map(Pattern::quote)
                .collect(Collectors.joining("|"));

        final String regex = String.format("(?:[\\s]|(^))(%s)(?:[(\\s]|($))", groups);
        final Pattern pattern = Pattern.compile(regex);
        final int groupToReplace = 2;

        final String withReplacements = replace(definition, using, replacer, groupToReplace, pattern);
        return replaceConcrete(withReplacements, using, replacer);
    }

    private static String replace(final String definition, final Map<String, From> using,
                                  final Function<Schema<?>, String> replacer,
                                  final int groupToReplace, Pattern pattern) {

        final StringBuilder stringBuilder = new StringBuilder(definition);
        final Matcher matcher = pattern.matcher(definition);
        if (matcher.find()) {
            String name = matcher.group(groupToReplace);

            final From use = using.getOrDefault(name, using.get(name.replaceAll("^\"|\"$", "")));
            if (use instanceof FromSchema) {
                stringBuilder.replace(matcher.start(groupToReplace), matcher.end(groupToReplace), replacer.apply(((FromSchema) use).getSchema()));
                return replace(stringBuilder.toString(), using, replacer, groupToReplace, pattern);
            } else {
                throw new IllegalStateException("SQL view schema does not declare " + name);
            }

        }

        return stringBuilder.toString();
    }
}
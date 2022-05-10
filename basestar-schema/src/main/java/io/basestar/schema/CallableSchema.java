package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import io.basestar.schema.from.From;
import io.basestar.schema.from.FromSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface CallableSchema extends Schema {

    Pattern DEFINITION_REPLACEMENT_REGEX = Pattern.compile("\"@\\{(.*?)}\"|@\\{(.*?)}");

    String getLanguage();

    List<Argument> getArguments();

    String getDefinition();

    default String getReplacedDefinition(final BiFunction<Schema, Boolean, String> replacer) {

        return getReplacedDefinition(getDefinition(), getUsing(), replacer);
    }

    Map<String, From> getUsing();

    interface Descriptor<S extends CallableSchema> extends Schema.Descriptor<S> {

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        String getLanguage();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Argument.Descriptor> getArguments();

        String getDefinition();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, From.Descriptor> getUsing();

        interface Self<S extends CallableSchema> extends Schema.Descriptor.Self<S>, Descriptor<S> {

            @Override
            default String getLanguage() {

                return self().getLanguage();
            }

            default List<Argument.Descriptor> getArguments() {

                return Immutable.transform(self().getArguments(), Argument::descriptor);
            }

            @Override
            default String getDefinition() {

                return self().getDefinition();
            }

            default Map<String, From.Descriptor> getUsing() {

                return Immutable.transformValues(self().getUsing(), (k, v) -> v.descriptor());
            }
        }
    }

    @SuppressWarnings("unused")
    interface Builder<B extends Schema.Builder<B, S>, S extends CallableSchema> extends Schema.Builder<B, S>, Descriptor<S> {

        B setLanguage(String language);

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        B setArguments(List<Argument.Descriptor> arguments);

        B setDefinition(String definition);

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        B setUsing(Map<String, From.Descriptor> using);
    }

    @Override
    default void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        final Name qualifiedName = getQualifiedName();
        final Map<String, From> using = getUsing();
        if (!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            using.forEach((k, v) -> v.collectDependencies(out));
        }
    }

    @Override
    default void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        final Name qualifiedName = getQualifiedName();
        final Map<String, From> using = getUsing();
        if (!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            using.forEach((k, v) -> v.collectMaterializationDependencies(out));
        }
    }

    static String replaceConcrete(final String definition, final Map<String, From> using,
                                  final BiFunction<Schema, Boolean, String> replacer) {

        final Matcher matcher = DEFINITION_REPLACEMENT_REGEX.matcher(definition);
        final StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            final String name = Nullsafe.orDefault(matcher.group(1), matcher.group(2));
            final From use = using.get(name);
            if (use instanceof FromSchema) {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacer.apply(((FromSchema) use).getSchema(), ((FromSchema) use).getVersioned())));
            } else {
                throw new IllegalStateException("SQL view schema does not declare " + name);
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }


    static String getReplacedDefinition(final String definition, final Map<String, From> using,
                                        final BiFunction<Schema, Boolean, String> replacer) {

        if (using.keySet().size() == 0) {
            return definition;
        }
        final String groups = using.keySet().stream()
                .flatMap(name -> Stream.of(name, "\"" + name + "\""))
                .map(Pattern::quote)
                .collect(Collectors.joining("|"));

        final String regex = String.format("(?:[\\s(]|(^))(%s)(?:[(\\s;,]|($))", groups);
        final Pattern pattern = Pattern.compile(regex);
        final int groupToReplace = 2;

        final String withReplacements = replace(definition, using, replacer, groupToReplace, pattern);
        return replaceConcrete(withReplacements, using, replacer);
    }

    static String replace(final String definition, final Map<String, From> using,
                          final BiFunction<Schema, Boolean, String> replacer,
                          final int groupToReplace, Pattern pattern) {

        final StringBuilder stringBuilder = new StringBuilder(definition);
        final Matcher matcher = pattern.matcher(definition);
        if (matcher.find()) {
            String name = matcher.group(groupToReplace);

            final From use = using.getOrDefault(name, using.get(name.replaceAll("^\"|\"$", "")));
            if (use instanceof FromSchema) {
                stringBuilder.replace(matcher.start(groupToReplace), matcher.end(groupToReplace), replacer.apply(((FromSchema) use).getSchema(), ((FromSchema) use).getVersioned()));
                return replace(stringBuilder.toString(), using, replacer, groupToReplace, pattern);
            } else {
                throw new IllegalStateException("SQL view schema does not declare " + name);
            }

        }

        return stringBuilder.toString();
    }
}

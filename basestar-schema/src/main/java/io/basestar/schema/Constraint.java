package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.*;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.jsr380.groups.Error;
import io.basestar.schema.jsr380.groups.*;
import io.basestar.schema.use.Use;
import io.basestar.schema.validation.Validation;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.Payload;
import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@Slf4j
@JsonSerialize(using = Constraint.Serializer.class)
@JsonDeserialize(using = Constraint.Deserializer.class)
public class Constraint implements Serializable {

    public static BiMap<String, Class<?>> GROUP_CLASSES = ImmutableBiMap.<String, Class<?>>builder()
            .put(Default.NAME, Default.class)
            .put(Info.NAME, Info.class)
            .put(Warning.NAME, Warning.class)
            .put(Error.NAME, Error.class)
            .put(Fatal.NAME, Fatal.class)
            .build();

    private static final String MESSAGE = "message";

    private static final String CONDITIONS = "when";

    private static final String GROUPS = "groups";

    public static final String REQUIRED = "required";

    public static final String IMMUTABLE = "immutable";

    @Nonnull
    private final Validation.Validator validator;

    @Nullable
    @JsonProperty(MESSAGE)
    private final String message;

    @Nonnull
    @JsonProperty(CONDITIONS)
    private final List<Expression> when;

    private final Set<String> groups;

    private Constraint(final Validation.Validator validator, final String message, final List<Expression> when, final Set<String> groups) {

        this.validator = validator;
        this.message = message;
        this.when = Immutable.list(when);
        this.groups = Immutable.set(groups);
    }

    public List<Violation> violations(final Use<?> type, final Context context, final Name name, final Object value) {

        if(validator.validate(type, context, value)) {
            return ImmutableList.of();
        } else {
            return ImmutableList.of(new Violation(name, validator.type(), message, groups));
        }
    }

    public Annotation toJsr380(final Use<?> type) {

        return toJsr380(type, getMessage());
    }

    public Annotation toJsr380(final Use<?> type, final String message) {

        final Class<?>[] groupClasses = this.groups.stream()
                .map(v -> GROUP_CLASSES.get(v))
                .filter(Objects::nonNull).toArray(Class<?>[]::new);
        return toJsr380(type, message, groupClasses, null);
    }

    public Annotation toJsr380(final Use<?> type, final String message, final Class<?>[] groups, final Class<? extends Payload>[] payload) {

        final ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
        if(message != null) {
            values.put("message", message);
        }
        if(groups != null) {
            values.put("groups", groups);
        }
        if(payload != null) {
            values.put("payload", payload);
        }
        return validator.toJsr380(type, values.build());
    }

    @Data
    public static class Violation {

        @JsonSerialize(using = ToStringSerializer.class)
        private final Name name;

        private final String type;

        @Nullable
        private final String message;

        @JsonIgnore
        private final Set<String> groups;

        @JsonProperty("groups")
        public Set<String> getEffectiveGroups() {

            if(groups == null || groups.isEmpty()) {
                return ImmutableSet.of(Default.NAME);
            } else {
                return groups;
            }
        }
    }

    public static Constraint of(final Validation.Validator validator) {

        return of(validator, null);
    }

    public static Constraint of(final Validation.Validator validator, final String message) {

        return new Constraint(validator, message, null, ImmutableSet.of());
    }

    public static Constraint of(final Validation.Validator validator, final String message, final List<Expression> conditions, final Set<String> groups) {

        return new Constraint(validator, message, conditions, groups);
    }

    public static Optional<Constraint> fromJsr380(final Use<?> type, final Annotation annotation, final String message, final Class<?>[] groups) {

        final Map<Class<?>, String> classToName = GROUP_CLASSES.inverse();
        final Set<String> groupNames = Stream.of(groups)
                .map(classToName::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        return Validation.createJsr380Validator(type, annotation).map(validator -> of(validator, message, null, groupNames));
    }

    public static class Serializer extends JsonSerializer<Constraint> {

        @Override
        public void serialize(final Constraint constraint, final JsonGenerator generator, final SerializerProvider serializerProvider) throws IOException {

            final String message = constraint.getMessage();
            final Validation.Validator validator = constraint.getValidator();
            final List<Expression> when = constraint.getWhen();
            final Set<String> groups = constraint.getGroups();
            generator.writeStartObject();
            generator.writeObjectField(validator.type(), validator.shorthand());
            if(message != null) {
                generator.writeStringField(MESSAGE, message);
            }
            if(!when.isEmpty()) {
                if(when.size() == 1) {
                    generator.writeStringField(CONDITIONS, when.get(0).toString());
                } else {
                    generator.writeArrayFieldStart(CONDITIONS);
                    for(final Expression entry : when) {
                        generator.writeString(entry.toString());
                    }
                    generator.writeEndObject();
                }
            }
            if(!groups.isEmpty()) {
                if(groups.size() == 1) {
                    generator.writeStringField(GROUPS, groups.iterator().next());
                } else {
                    generator.writeArrayFieldStart(GROUPS);
                    for(final String group : groups) {
                        generator.writeString(group);
                    }
                    generator.writeEndObject();
                }
            }
            generator.writeEndObject();
        }
    }

    public static class Deserializer extends JsonDeserializer<Constraint> {

        @Override
        public Constraint deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {

            String message = null;
            Validation.Validator validator = null;
            final List<Expression> when = new ArrayList<>();
            final Set<String> groups = new HashSet<>();

            if(parser.getCurrentToken() != JsonToken.START_OBJECT) {
                throw context.wrongTokenException(parser, Constraint.class, JsonToken.START_OBJECT, null);
            }
            while(parser.nextToken() == JsonToken.FIELD_NAME) {
                final String name = parser.currentName();
                parser.nextToken();
                if (MESSAGE.equals(name)) {
                    message = parser.getText();
                } else if(CONDITIONS.equals(name)) {
                    if(parser.getCurrentToken() == JsonToken.START_ARRAY) {
                        while(parser.nextToken() != JsonToken.END_ARRAY) {
                            when.add(parser.readValueAs(Expression.class));
                        }
                        if(parser.getCurrentToken() != JsonToken.END_ARRAY) {
                            throw context.wrongTokenException(parser, Constraint.class, JsonToken.END_OBJECT, null);
                        }
                    } else {
                        when.add(parser.readValueAs(Expression.class));
                    }
                } else if(GROUPS.equals(name)) {
                    if(parser.getCurrentToken() == JsonToken.START_ARRAY) {
                        while(parser.nextToken() != JsonToken.END_ARRAY) {
                            groups.add(parser.readValueAs(String.class));
                        }
                        if(parser.getCurrentToken() != JsonToken.END_ARRAY) {
                            throw context.wrongTokenException(parser, Constraint.class, JsonToken.END_OBJECT, null);
                        }
                    } else {
                        groups.add(parser.readValueAs(String.class));
                    }
                } else if(validator == null) {
                    final Validation validation = Validation.forType(name);
                    validator = parser.readValueAs(validation.validatorClass());
                } else {
                    throw new IllegalStateException("Already have a validator of type " + validator.type() + " cannot add " + name);
                }
            }
            if(parser.getCurrentToken() != JsonToken.END_OBJECT) {
                throw context.wrongTokenException(parser, Constraint.class, JsonToken.END_OBJECT, null);
            }
            if(validator != null) {
                return Constraint.of(validator, message, when, groups);
            } else {
                throw new JsonParseException(parser, "Constraint must have one of: " + Validation.types());
            }
        }
    }
}

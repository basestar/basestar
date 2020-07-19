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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import io.basestar.schema.validation.Validation;
import io.basestar.util.Name;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.Payload;
import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Optional;

@Data
@Slf4j
@JsonSerialize(using = Constraint.Serializer.class)
@JsonDeserialize(using = Constraint.Deserializer.class)
public class Constraint implements Serializable {

    public static final String REQUIRED = "required";

    public static final String IMMUTABLE = "immutable";

    @Nonnull
    private final Validation.Validator validator;

    @Nullable
    private final String message;

    private Constraint(final Validation.Validator validator, final String message) {

        this.validator = validator;
        this.message = message;
    }

    public List<Violation> violations(final Use<?> type, final Context context, final Name name, final Object value) {

        if(validator.validate(type, context, value)) {
            return ImmutableList.of();
        } else {
            return ImmutableList.of(new Violation(name, validator.type(), message));
        }
    }

    public Annotation toJsr380(final Use<?> type) {

        return toJsr380(type, getMessage());
    }

    @SuppressWarnings("unchecked")
    public Annotation toJsr380(final Use<?> type, final String message) {

        return toJsr380(type, message, null, null);
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
    }

    public static Constraint of(final Validation.Validator validator) {

        return of(validator, null);
    }

    public static Constraint of(final Validation.Validator validator, final String message) {

        return new Constraint(validator, message);
    }

    public static Optional<Constraint> fromJsr380(final Use<?> type, final Annotation annotation, final String message) {

        return Validation.createJsr380Validator(type, annotation).map(validator -> of(validator, message));
    }

    public static class Serializer extends JsonSerializer<Constraint> {

        @Override
        public void serialize(final Constraint constraint, final JsonGenerator generator, final SerializerProvider serializerProvider) throws IOException {

            final String message = constraint.getMessage();
            final Validation.Validator validator = constraint.getValidator();
            generator.writeStartObject();
            generator.writeObjectField(validator.type(), validator.shorthand());
            if(message != null) {
                generator.writeStringField("message", message);
            }
            generator.writeEndObject();
        }
    }

    public static class Deserializer extends JsonDeserializer<Constraint> {

        @Override
        public Constraint deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {

            String message = null;
            Validation.Validator validator = null;

            if(parser.getCurrentToken() != JsonToken.START_OBJECT) {
                throw context.wrongTokenException(parser, Constraint.class, JsonToken.START_OBJECT, null);
            }
            while(parser.nextToken() == JsonToken.FIELD_NAME) {
                final String name = parser.currentName();
                parser.nextToken();
                if ("message".equals(name)) {
                    message = parser.getText();
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
                return Constraint.of(validator, message);
            } else {
                throw new JsonParseException(parser, "Constraint must have one of: " + Validation.types());
            }
        }
    }
}

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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.validator.*;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Getter
public class Constraint implements Named, Described, Serializable {

    public static final String REQUIRED = "required";

    public static final String IMMUTABLE = "immutable";

    @Nonnull
    private final Name qualifiedName;

    @Nullable
    private final String description;

    @Nullable
    private final String message;

    @Nonnull
    private final List<Validator> validators;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Described {

        String getMessage();

        @JsonIgnore
        List<Validator> getValidators();

        @JsonAnyGetter
        @SuppressWarnings("unused")
        default Map<String, Validator> getValidatorMap() {

            return getValidators().stream().collect(Collectors.toMap(
                    Validator::type, v -> v
            ));
        }

        default Constraint build(final Name qualifiedName) {

            return new Constraint(this, qualifiedName);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        @Nullable
        private String description;

        @Nullable
        private String message;

        @Nullable
        @JsonProperty(RangeValidator.TYPE)
        private RangeValidator range;

        @Nullable
        @JsonProperty(SizeValidator.TYPE)
        private SizeValidator size;

        @Nullable
        @JsonProperty(RegexValidator.TYPE)
        private RegexValidator regex;

        @Nullable
        @JsonProperty(ExpressionValidator.TYPE)
        private ExpressionValidator expression;

        @Override
        public Map<String, Validator> getValidatorMap() {

            return ImmutableMap.of();
        }

        @Override
        public List<Validator> getValidators() {

            return Stream.of(range, size, regex, expression)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private Constraint(final Descriptor descriptor, final Name qualifiedName) {

        this.qualifiedName = qualifiedName;
        this.description = descriptor.getDescription();
        this.validators = Nullsafe.immutableCopy(descriptor.getValidators());
        this.message = descriptor.getMessage();
        if(validators.isEmpty()) {
            throw new SchemaValidationException(qualifiedName, "Constraint must have at least one validator");
        }
    }

    public List<Violation> violations(final Context context, final Name name, final String constraint, final Object value) {

        final List<Violation> violations = new ArrayList<>();
        for(final Validator validator : validators) {
            if(!validator.validate(context, value)) {
                violations.add(new Violation(name, constraint, message(validator, message)));
            }
        }
        return violations;
    }

    protected static String message(final Validator validator, final String message) {

        if(message != null) {
            return message;
        } else {
            return validator.defaultMessage();
        }
    }

    @Data
    public static class Violation {

        @JsonSerialize(using = ToStringSerializer.class)
        private final Name name;

        private final String constraint;

        @Nullable
        private final String message;
    }

    public Descriptor descriptor() {

        return new Descriptor() {
            @Override
            public String getMessage() {

                return message;
            }

            @Override
            public List<Validator> getValidators() {

                return validators;
            }

            @Nullable
            @Override
            public String getDescription() {

                return description;
            }
        };
    }
}

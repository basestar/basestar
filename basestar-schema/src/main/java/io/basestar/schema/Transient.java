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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.jackson.serde.PathDeserializer;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Transient
 */

@Getter
@Accessors(chain = true)
public class Transient implements Member {

    @Nonnull
    private final String name;

    @Nullable
    private final String description;

    @Nonnull
    private final Expression expression;

    @Nullable
    private final Visibility visibility;

    @Nonnull
    private final SortedSet<Path> expand;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Described {

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression expression;

        @Nullable
        @JsonSetter(contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = PathDeserializer.class)
        private Set<Path> expand;

        @Nullable
        private Visibility visibility;

        public Transient build(final String name) {

            return new Transient(this, name);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private Transient(final Builder builder, final String name) {

        this.name = name;
        this.description = builder.getDescription();
        this.expression =  Nullsafe.of(builder.getExpression());
        this.visibility = builder.getVisibility();
        this.expand = Nullsafe.immutableSortedCopy(builder.getExpand());
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
    }

    public Object evaluate(final Context context) {

        return expression.evaluate(context);
    }

    @Override
    public Object expand(final Object value, final Expander expander, final Set<Path> expand) {

        return value;
    }

    @Override
    public Object applyVisibility(final Context context, final Object value) {

        return value;
    }

    @Override
    public Object evaluateTransients(final Context context, final Object value, final Set<Path> expand) {

        return expression.evaluate(context);
    }

    //FIXME
    @Override
    public Use<?> typeOf(final Path path) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Path> transientExpand(final Path path, final Set<Path> expand) {

        final Set<Path> result = new HashSet<>();
        this.expand.forEach(p -> {
            if(p.isChild(Path.of(Schema.VAR_THIS))) {
                // Move expand from this to expand on the parameter path
                result.add(path.with(p.withoutFirst()));
            } else {
                result.add(p);
            }
        });
        return result;
    }

    @Override
    public Set<Path> requireExpand(final Set<Path> paths) {

        return Collections.emptySet();
    }

    public interface Resolver {

        Map<String, Transient> getDeclaredTransients();

        Map<String, Transient> getAllTransients();

        default Transient getTransient(final String name, final boolean inherited) {

            if(inherited) {
                return getAllTransients().get(name);
            } else {
                return getDeclaredTransients().get(name);
            }
        }

        default Transient requireTransient(final String name, final boolean inherited) {

            final Transient result = getTransient(name, inherited);
            if (result == null) {
                throw new MissingMemberException(name);
            } else {
                return result;
            }
        }
    }
}

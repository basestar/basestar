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
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.util.Nullsafe;
import io.basestar.util.PagedList;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Link
 */

@Getter
@Accessors(chain = true)
public class Link implements Member {

    @Nonnull
    private final String name;

    @Nullable
    private final String description;

    @Nonnull
    private final ObjectSchema schema;

    @Nonnull
    private final Expression expression;

    @Nonnull
    private final List<Sort> sort;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Described {

        @Nullable
        private String schema;

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression expression;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        public Link build(final Schema.Resolver resolver, final String name) {

            return new Link(this, resolver, name);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public Link(final Builder builder, final Schema.Resolver resolver, final String name) {

        this.name = name;
        this.description = builder.getDescription();
        this.schema = resolver.requireObjectSchema(builder.getSchema());
        this.expression = Nullsafe.of(builder.getExpression());
        this.sort = Nullsafe.immutableCopy(builder.getSort());
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object expand(final Object value, final Expander expander, final Set<Path> expand) {

        if(expand == null) {
            return null;
        } else {
            return expander.link(this, (PagedList<Instance>)value, expand);
        }
    }

    @Override
    @Deprecated
    public Set<Path> requireExpand(final Set<Path> paths) {

        final Set<Path> result = new HashSet<>();
        result.add(Path.empty());
        result.addAll(schema.requiredExpand(paths));
        return result;
    }

    //FIXME
    @Override
    public Use<?> typeOf(final Path path) {

        throw new UnsupportedOperationException();
    }

    public interface Resolver {

        Map<String, Link> getDeclaredLinks();

        Map<String, Link> getAllLinks();

        default Link getLink(final String name, final boolean inherited) {

            if(inherited) {
                return getAllLinks().get(name);
            } else {
                return getDeclaredLinks().get(name);
            }
        }

        default Link requireLink(final String name, final boolean inherited) {

            final Link result = getLink(name, inherited);
            if (result == null) {
                throw new MissingMemberException(name);
            } else {
                return result;
            }
        }
    }
}

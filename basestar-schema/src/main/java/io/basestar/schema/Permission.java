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
import io.basestar.expression.constant.Constant;
import io.basestar.expression.logical.And;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Permission
 *
 * Permissions describe - using expressions - the rules for reading, creating, updating and deleting objects.
 *
 * The variables available in the context of a permission expression depend on the type of the expression, as follows:
 *
 * - Read
 *    - `this` the object as it currently appears
 * - Create
 *    - `after` the object as it would appear if it were successfully created
 * - Update
 *    - `before` the object as it currently appears
 *    - `after` the object as it would appear if it were successfully updated
 * - Delete
 *    - `before` the object as it currently appears
 */

@Getter
public class Permission implements Serializable {

    public static final String READ = "read";

    public static final String CREATE = "create";

    public static final String UPDATE = "update";

    public static final String DELETE = "delete";

    @Nonnull
    private final String name;

    @Nullable
    private final String description;

    private final boolean anonymous;

    @Nonnull
    private final Expression expression;

    @Nonnull
    private final SortedSet<Name> expand;

    @Nonnull
    private final SortedSet<Name> inherit;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Described {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Boolean getAnonymous();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Expression getExpression();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<Name> getExpand();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<Name> getInherit();

        default Permission build(final String name) {

            return new Permission(this, name);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        @Nullable
        private Boolean anonymous;

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression expression;

        @Nullable
        @JsonSetter(contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private Set<Name> expand;

        @Nullable
        @JsonSetter(contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private Set<Name> inherit;
    }

    public static Builder builder() {

        return new Builder();
    }

    private Permission(final Descriptor descriptor, final String name) {

        this.name = name;
        this.description = descriptor.getDescription();
        this.anonymous = Nullsafe.orDefault(descriptor.getAnonymous(), false);
        this.expression = Nullsafe.orDefault(descriptor.getExpression(), Constant.TRUE);
        this.expand = Immutable.sortedSet(descriptor.getExpand());
        this.inherit = Immutable.sortedSet(descriptor.getInherit());
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
    }

    // Merge permissions

    private Permission(final Permission a, final Permission b) {

        this.name = b.getName();
        this.description = b.getDescription();
        this.anonymous = a.isAnonymous() && b.isAnonymous();
        this.expression = new And(a.getExpression(), b.getExpression());
        final SortedSet<Name> expand = new TreeSet<>();
        expand.addAll(a.getExpand());
        expand.addAll(b.getExpand());
        this.expand = Collections.unmodifiableSortedSet(expand);
        final SortedSet<Name> inherit = new TreeSet<>();
        inherit.addAll(a.getExpand());
        inherit.addAll(b.getExpand());
        this.inherit = Collections.unmodifiableSortedSet(inherit);
    }

    public Permission extend(final Permission ext) {

        return new Permission(this, ext);
    }

    public static SortedMap<String, Permission> extend(final Map<String, Permission> base, final Map<String, Permission> ext) {

        return Immutable.sortedMerge(base, ext, Permission::extend);
    }

    public static SortedMap<String, Permission> extend(final Collection<? extends Resolver> base, final Map<String, Permission> ext) {

        return Immutable.sortedMap(Stream.concat(
                base.stream().map(Resolver::getPermissions),
                Stream.of(ext)
        ).reduce(Permission::extend).orElse(Collections.emptyMap()));
    }

    public interface Resolver {

        interface Descriptor {

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Map<String, Permission.Descriptor> getPermissions();
        }

        interface Builder<B extends Builder<B>> extends Descriptor {

            default B setPermission(final String name, final Permission.Descriptor v) {

                return setPermissions(Immutable.put(getPermissions(), name, v));
            }

            B setPermissions(Map<String, Permission.Descriptor> vs);
        }

        Map<String, Permission> getDeclaredPermissions();

        Map<String, Permission> getPermissions();

        default Map<String, Permission.Descriptor> describeDeclaredPermissions() {

            return getDeclaredPermissions().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().descriptor()
            ));
        }

        default Permission getPermission(final String name) {

            return getPermission(name, true);
        }

        default Permission getPermission(final String name, final boolean inherited) {

            if(inherited) {
                return getPermissions().get(name);
            } else {
                return getDeclaredPermissions().get(name);
            }
        }
    }

    public Descriptor descriptor() {

        return new Descriptor() {
            @Override
            public Boolean getAnonymous() {

                return anonymous;
            }

            @Override
            public Expression getExpression() {

                return expression;
            }

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Set<Name> getInherit() {

                return inherit;
            }

            @Nullable
            @Override
            public String getDescription() {

                return description;
            }
        };
    }
}

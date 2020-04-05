package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Expression;
import io.basestar.expression.logical.And;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.jackson.serde.PathDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Permission
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
    private final SortedSet<Path> expand;

    @Nonnull
    private final SortedSet<Path> inherit;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Described {

        @Nullable
        private Boolean anonymous;

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
        @JsonSetter(contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = PathDeserializer.class)
        private Set<Path> inherit;

        public Permission build(final String name) {

            return new Permission(this, name);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private Permission(final Builder builder, final String name) {

        this.name = name;
        this.description = builder.getDescription();
        this.anonymous = Nullsafe.of(builder.getAnonymous(), false);
        this.expression = Nullsafe.of(builder.getExpression());
        this.expand = Nullsafe.immutableSortedCopy(builder.getExpand());
        this.inherit = Nullsafe.immutableSortedCopy(builder.getInherit());
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
        final SortedSet<Path> expand = new TreeSet<>();
        expand.addAll(a.getExpand());
        expand.addAll(b.getExpand());
        this.expand = Collections.unmodifiableSortedSet(expand);
        final SortedSet<Path> inherit = new TreeSet<>();
        inherit.addAll(a.getExpand());
        inherit.addAll(b.getExpand());
        this.inherit = Collections.unmodifiableSortedSet(inherit);
    }

    public Permission merge(final Permission b) {

        return new Permission(this, b);
    }
}

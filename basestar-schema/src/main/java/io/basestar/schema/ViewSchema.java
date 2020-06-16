package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseCollection;
import io.basestar.schema.use.UseMap;
import io.basestar.schema.use.UseRef;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class ViewSchema implements InstanceSchema, Permission.Resolver {

    @Nonnull
    private final String name;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    @Nonnull
    private final InstanceSchema from;

    @Nonnull
    private final List<Sort> sort;

    /** Description of the schema */

    @Nullable
    private final String description;

    @Nonnull
    private final SortedMap<String, Property> select;

    @Nonnull
    private final SortedMap<String, Property> group;

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    @Nonnull
    private final SortedMap<String, Permission> declaredPermissions;

    @Nonnull
    private final SortedMap<String, Object> extensions;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "from", "select", "group", "permissions", "extensions"})
    public static class Builder implements InstanceSchema.Builder {

        public static final String TYPE = "view";

        @Nullable
        private Long version;

        @Nullable
        private String description;

        @Nullable
        private String from;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Builder> select;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Builder> group;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Permission.Builder> permissions;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

        public String getType() {

            return TYPE;
        }

        public Builder setSelect(final String name, final Property.Builder v) {

            select = Nullsafe.immutableCopyPut(select, name, v);
            return this;
        }

        public Builder setGroup(final String name, final Property.Builder v) {

            group = Nullsafe.immutableCopyPut(group, name, v);
            return this;
        }

        public Builder setPermission(final String name, final Permission.Builder v) {

            permissions = Nullsafe.immutableCopyPut(permissions, name, v);
            return this;
        }

        // FIXME:
        @Override
        public InstanceSchema.Builder setProperty(final String name, final Property.Builder v) {

            throw new UnsupportedOperationException();
        }

        // FIXME:
        @Override
        public InstanceSchema.Builder setProperties(final Map<String, Property.Builder> vs) {

            throw new UnsupportedOperationException();
        }

        @Override
        public ViewSchema build(final Resolver resolver, final String name, final int slot) {

            return new ViewSchema(this, resolver, name, slot);
        }

        @Override
        public ViewSchema build() {

            return new ViewSchema(this, name -> null, Schema.anonymousName(), Schema.anonymousSlot());
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private ViewSchema(final Builder builder, final Schema.Resolver resolver, final String name, final int slot) {

        resolver.constructing(this);
        this.name = name;
        this.slot = slot;
        this.version = Nullsafe.option(builder.getVersion(), 1L);
        this.from = resolver.requireInstanceSchema(builder.getFrom());
        this.sort = Nullsafe.immutableCopy(builder.getSort());
        this.description = builder.getDescription();
        this.select = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getSelect()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        this.group = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getGroup()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        this.declaredProperties = ImmutableSortedMap.<String, Property>orderedBy(Ordering.natural())
                .putAll(select).putAll(group).build();
        this.declaredPermissions = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getPermissions()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.extensions = Nullsafe.immutableSortedCopy(builder.getExtensions());
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
        this.declaredProperties.forEach(ViewSchema::validateProperty);
    }

    private static void validateProperty(final String name, final Property property) {

        if(!property.getConstraints().isEmpty()) {
            throw new SchemaValidationException("View properties cannot have constraints (" + name + ")");
        }
        if(property.getExpression() == null) {
            throw new SchemaValidationException("Every view property must have an expression (" + name + ")");
        }
        property.getType().visit(TypeValidator.INSTANCE);
    }

    @Override
    public Instance create(final Map<String, Object> value, final boolean expand, final boolean suppress) {

        return new Instance(readProperties(value, expand, suppress));
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Path path, final Instance after) {

        return Collections.emptySet();
    }

    @Override
    public Map<String, Property> getProperties() {

        return declaredProperties;
    }

    @Override
    public Map<String, Permission> getPermissions() {

        return declaredPermissions;
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return ImmutableSortedMap.of();
    }

    @Override
    public InstanceSchema getExtend() {

        return null;
    }

    @Override
    public boolean isConcrete() {

        return true;
    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        return getDeclaredProperties();
    }

    @Override
    public Map<String, ? extends Member> getMembers() {

        return getProperties();
    }

    @Override
    public Member getMember(final String name, final boolean inherited) {

        return getProperty(name, inherited);
    }

    private static class TypeValidator implements Use.Visitor.Defaulting<Void> {

        private static final TypeValidator INSTANCE = new TypeValidator();

        @Override
        public Void visitDefault(final Use<?> type) {

            return null;
        }

        @Override
        public Void visitRef(final UseRef type) {

            throw new SchemaValidationException("View properties cannot use references");
        }

        @Override
        public <T> Void visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

            return type.getType().visit(this);
        }

        @Override
        public <T> Void visitMap(final UseMap<T> type) {

            return type.getType().visit(this);
        }
    }
}

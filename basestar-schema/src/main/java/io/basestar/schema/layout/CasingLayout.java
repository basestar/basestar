package io.basestar.schema.layout;

import io.basestar.schema.Property;
import io.basestar.schema.StructSchema;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseCollection;
import io.basestar.schema.use.UseMap;
import io.basestar.schema.use.UseStruct;
import io.basestar.schema.util.Casing;
import io.basestar.util.Name;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CasingLayout implements Layout {

    private final Layout base;

    private final Casing casing;

    public CasingLayout(final Layout base, final Casing casing) {

        this.base = base;
        this.casing = casing;
    }

    @Override
    public Map<String, Use<?>> layoutSchema(final Set<Name> expand) {

        return layoutSchema(base, expand);
    }

    private Map<String, Use<?>> layoutSchema(final Layout base, final Set<Name> expand) {

        final Map<String, Use<?>> result = new HashMap<>();
        base.layoutSchema(expand).forEach((k, v) -> {
            result.put(casing.apply(k), layoutSchema(v));
        });
        return result;
    }

    private Use<?> layoutSchema(final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<Use<?>>() {

            @Override
            public Use<?> visitDefault(final Use<?> type) {

                return type;
            }

            @Override
            public <T> Use<?> visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

                return type.transform(v -> v.visit(this));
            }

            @Override
            public <T> Use<?> visitMap(final UseMap<T> type) {

                return type.transform(v -> v.visit(this));
            }

            @Override
            public Use<?> visitStruct(final UseStruct type) {

                final Map<String, Property.Descriptor> props = new HashMap<>();
                type.getSchema().getProperties().forEach((name, prop) -> {
                    final Property.Descriptor descriptor = prop.descriptor();
                    props.put(casing.apply(name), new Property.Descriptor.Delegating() {

                        @Override
                        public Property.Descriptor delegate() {

                            return descriptor;
                        }

                        @Override
                        public Use<?> getType() {

                            return layoutSchema(prop.getType());
                        }
                    });
                });
                return new UseStruct(StructSchema.builder()
                        .setProperties(props)
                        .build());
            }
        });
    }

    @Override
    public Map<String, Object> applyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return null;
    }

    @Override
    public Map<String, Object> unapplyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return null;
    }
}

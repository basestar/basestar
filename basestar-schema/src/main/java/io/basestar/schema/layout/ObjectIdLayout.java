package io.basestar.schema.layout;

import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseObject;
import io.basestar.schema.use.UseString;
import io.basestar.util.Name;
import lombok.Getter;

import java.util.Set;

/**
 * Layout transformation that converts object and view-record references to flat string fields containing the target id.
 *
 * This transformation works recursively through collections, maps and structs
 */


public class ObjectIdLayout implements Layout.Simple {

    @Getter
    private final Layout baseLayout;

    public ObjectIdLayout(final Layout baseLayout) {

        this.baseLayout = baseLayout;
    }

    @Override
    public Use<?> layoutSchema(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor.Transforming() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Use<?> transform(final Use<?> type, final Set<Name> expand) {

                return layoutSchema(type, expand);
            }

            @Override
            public Use<?> visitObject(final UseObject type) {

                return UseString.DEFAULT;
            }
        });
    }

    @Override
    public Object applyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.TransformingValue() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Object getValue() {

                return value;
            }

            @Override
            public Object transform(final Use<?> type, final Set<Name> expand, final Object value) {

                return applyLayout(type, expand, value);
            }

            @Override
            public Object visitObject(final UseObject type) {

                return Instance.getId(type.create(value));
            }
        });
    }

    @Override
    public Object unapplyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.TransformingValue() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Object getValue() {

                return value;
            }

            @Override
            public Object transform(final Use<?> type, final Set<Name> expand, final Object value) {

                return applyLayout(type, expand, value);
            }

            @Override
            public Object visitObject(final UseObject type) {

                return ObjectSchema.ref((String)value);
            }
        });
    }
}

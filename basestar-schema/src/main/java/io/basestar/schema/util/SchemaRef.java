package io.basestar.schema.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.Version;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchemaRef {

    private Name name;

    private Schema.Descriptor<?, ?> inline;

    public static SchemaRef withName(final Name name) {

        return new SchemaRef(name, null);
    }

    @JsonCreator
    @SuppressWarnings("unused")
    public static SchemaRef withName(final String name) {

        return withName(Name.parse(name));
    }

    @JsonCreator
    @SuppressWarnings("unused")
    public static SchemaRef withInline(final Schema.Descriptor<?, ?> inline) {

        return new SchemaRef(null, inline);
    }

    public LinkableSchema resolve(final Schema.Resolver.Constructing resolver) {

        if(inline != null) {
            return (LinkableSchema) inline.build(null, new Schema.Resolver.Constructing() {

                @Override
                public void constructing(final Schema<?> schema) {

                    // no action (anonymous schema construction)
                }

                @Nullable
                @Override
                public Schema<?> getSchema(final Name qualifiedName) {

                    return resolver.getSchema(qualifiedName);
                }

            }, Version.CURRENT, Schema.anonymousQualifiedName(), Schema.anonymousSlot());
        } else {
            return resolver.requireLinkableSchema(name);
        }
    }
}

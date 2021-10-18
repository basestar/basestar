package io.basestar.schema.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
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

    @JsonValue
    public Object toJson() {

        if(name != null) {
            return name;
        } else {
            return inline;
        }
    }

    public Schema<?> resolve(final Schema.Resolver.Constructing resolver) {

        if (inline != null) {
            return inline.build(null, new Schema.Resolver.Constructing() {

                @Override
                public void constructing(final Name qualifiedName, final Schema<?> schema) {

                    // no action (anonymous schema construction)
                }

                @Nullable
                @Override
                public Schema<?> getSchema(final Name qualifiedName) {

                    return resolver.getSchema(qualifiedName);
                }

            }, Version.CURRENT, Schema.anonymousQualifiedName(), Schema.anonymousSlot());
        } else {
            return resolver.requireSchema(name);
        }
    }
}

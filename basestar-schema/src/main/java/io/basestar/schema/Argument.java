package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.schema.use.Use;
import io.basestar.util.Immutable;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

@Data
@Accessors(chain = true)
public class Argument implements Described, Extendable {

    @Nonnull
    private final String name;

    @Nonnull
    private final Use<?> type;

    @Nullable
    private final String description;

    @Nonnull
    private final Map<String, Serializable> extensions;

    public Argument(final Descriptor builder, final Schema.Resolver.Constructing resolver) {

        this.name = Nullsafe.require(builder.getName());
        this.type = Nullsafe.require(builder.getType()).resolve(resolver);
        this.description = builder.getDescription();
        this.extensions = Immutable.map(builder.getExtensions());
    }

    public static Builder builder() {

        return new Builder();
    }

    public Descriptor descriptor() {

        return (Descriptor.Self) () -> Argument.this;
    }

    @JsonDeserialize(as = Builder.class)
    interface Descriptor extends Described, Extendable {

        String getName();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Use<?> getType();

        default Argument build(final Schema.Resolver.Constructing resolver) {

            return new Argument(this, resolver);
        }

        interface Self extends Descriptor {

            Argument self();

            @Override
            default String getName() {

                return self().getName();
            }

            @Override
            default Use<?> getType() {

                return self().getType();
            }

            @Override
            default String getDescription() {

                return self().getDescription();
            }

            @Override
            default Map<String, Serializable> getExtensions() {

                return self().getExtensions();
            }
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        @Nullable
        private String name;

        @Nullable
        private Use<?> type;

        @Nullable
        private String description;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Serializable> extensions;
    }

}

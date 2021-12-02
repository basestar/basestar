package io.basestar.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@JsonDeserialize(builder = SimpleCaller.Builder.class)
public class SimpleCaller implements Caller {

    private static final long serialVersionUID = 1L;

    private final boolean anon;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final boolean _super;

    private final Name schema;

    private final String id;

    private final Map<String, Serializable> claims;

    public boolean isSuper() {

        return _super;
    }

    private SimpleCaller(final Builder builder) {

        this.anon = builder.anon;
        this._super = builder._super;
        this.schema = builder.schema;
        this.id = builder.id;
        this.claims = serializableClaims(builder.claims);
    }

    private static Map<String, Serializable> serializableClaims(final Map<String, Object> input) {

        final Map<String, Serializable> output = new HashMap<>();
        if (input != null) {
            input.forEach((k, v) -> {
                if (v instanceof Serializable) {
                    output.put(k, (Serializable) v);
                }
            });
        }
        return Immutable.map(output);
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        @JsonDeserialize(using = NameDeserializer.class)
        private Name schema;

        private String id;

        private Map<String, Object> claims;

        private boolean anon;

        @JsonIgnore
        @Setter(AccessLevel.NONE)
        private boolean _super;

        public Builder setSuper(final boolean value) {

            this._super = value;
            return this;
        }

        public SimpleCaller build() {

            return new SimpleCaller(this);
        }
    }
}

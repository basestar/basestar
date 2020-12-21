package io.basestar.schema;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableList;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.util.BucketFunction;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

@Data
@JsonDeserialize(builder = Bucketing.Builder.class)
public class Bucketing implements Serializable {

    public static final int DEFAULT_COUNT = 100;

    public static final BucketFunction DEFAULT_FUNCTION = BucketFunction.MURMER3_32;

    private final List<Name> using;

    private final int count;

    private final BucketFunction function;

    public Bucketing(final Name using) {

        this(ImmutableList.of(using));
    }

    public Bucketing(final List<Name> using) {

        this(using, DEFAULT_COUNT);
    }

    public Bucketing(final List<Name> using, final int count) {

        this(using, count, DEFAULT_FUNCTION);
    }

    public Bucketing(final List<Name> using, final int count, final BucketFunction function) {

        this.using = Immutable.copy(using);
        if(this.using.isEmpty()) {
            throw new SchemaValidationException("Bucket using must be set");
        }
        this.function = Nullsafe.require(function);
        this.count = count;
    }

    public static Builder builder() {

        return new Builder();
    }

    public int apply(final Function<Name, Object> fn) {

        final Object[] values = new Object[using.size()];
        for(int i = 0; i != using.size(); ++i) {
            values[i] = fn.apply(using.get(i));
        }
        return function.apply(count, values);
    }

    @Data
    public static class Builder {

        @JsonDeserialize(using = AbbrevListDeserializer.class, contentUsing = NameDeserializer.class)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        private List<Name> using;

        private Integer count;

        private BucketFunction function;

        public Bucketing build() {

            return new Bucketing(using,
                    Nullsafe.orDefault(count, DEFAULT_COUNT),
                    Nullsafe.orDefault(function, DEFAULT_FUNCTION));
        }
    }
}

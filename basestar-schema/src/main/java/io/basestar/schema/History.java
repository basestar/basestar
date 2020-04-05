package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Data;

import java.io.Serializable;

@Data
public class History implements Serializable {

    // Enabled with default (storage best) consistency
    public static final History ENABLED = new History(true);

    public static final History DISABLED = new History(false);

    public static final String ENABLED_VALUE = "ENABLED";

    public static final String DISABLED_VALUE = "DISABLED";

    private final boolean enabled;

    private final Consistency consistency;

    public History(final boolean enabled) {

        this(enabled, null);
    }

    public History(final boolean enabled, final Consistency consistency) {

        this.enabled = enabled;
        this.consistency = consistency;
    }

    public Consistency getConsistency(final Consistency defaultValue) {

        if(consistency == null) {
            return defaultValue;
        } else {
            return consistency;
        }
    }

    @JsonCreator
    public static History fromString(final String str) {

        final String upper = str.toUpperCase();
        if(upper.equals(ENABLED_VALUE)) {
            return ENABLED;
        } else if(upper.equals(DISABLED_VALUE)) {
            return DISABLED;
        } else {
            return new History(true, Consistency.valueOf(upper));
        }
    }
}

package io.basestar.stream;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
class TestSubscriptionInfo implements SubscriptionInfo {

    @JsonSerialize(contentUsing = ToStringSerializer.class)
    @JsonDeserialize(contentUsing = NameDeserializer.class)
    private Set<Name> expand;
}

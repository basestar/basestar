package io.basestar.graphql.subscription;

import io.basestar.stream.SubscriptionInfo;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GraphQLSubscriptionInfo implements SubscriptionInfo {

    private String alias;

    private Set<Name> names;
}

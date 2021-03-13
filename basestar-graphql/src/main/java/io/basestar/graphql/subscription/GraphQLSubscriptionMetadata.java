package io.basestar.graphql.subscription;

import io.basestar.stream.SubscriptionMetadata;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GraphQLSubscriptionMetadata implements SubscriptionMetadata {

    private String alias;

    private Set<Name> expand;

    private boolean query;
}

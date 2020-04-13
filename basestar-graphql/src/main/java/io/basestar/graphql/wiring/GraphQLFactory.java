package io.basestar.graphql.wiring;

import graphql.GraphQL;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.basestar.database.Database;
import io.basestar.schema.Namespace;

public class GraphQLFactory {

    private final Database database;

    private final Namespace namespace;

    public GraphQLFactory(final Database database, final Namespace namespace) {

        this.database = database;
        this.namespace = namespace;
    }

    public GraphQL graphQL() {

        final TypeDefinitionRegistry registry = new TypeDefinitionRegistryFactory(namespace).typeDefinitionRegistry();
        final RuntimeWiring wiring = new RuntimeWiringFactory(database, namespace).runtimeWiring();

        final SchemaGenerator generator = new SchemaGenerator();
        final GraphQLSchema schema = generator.makeExecutableSchema(registry, wiring);

        return GraphQL.newGraphQL(schema)
                .subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
                .build();
    }
}

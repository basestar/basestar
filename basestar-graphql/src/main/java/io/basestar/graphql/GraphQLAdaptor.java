package io.basestar.graphql;

/*-
 * #%L
 * basestar-graphql
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import graphql.GraphQL;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.basestar.database.Database;
import io.basestar.graphql.schema.SchemaAdaptor;
import io.basestar.graphql.wiring.RuntimeWiringFactory;
import io.basestar.schema.Namespace;

public class GraphQLAdaptor {

    private final Database database;

    private final Namespace namespace;

    private final GraphQLStrategy strategy;

    public GraphQLAdaptor(final Database database, final Namespace namespace) {

        this(database, namespace, GraphQLStrategy.DEFAULT);
    }

    public GraphQLAdaptor(final Database database, final Namespace namespace, final GraphQLStrategy strategy) {

        this.database = database;
        this.namespace = namespace;
        this.strategy = strategy;
    }

    public GraphQL graphQL() {

        final TypeDefinitionRegistry registry = new SchemaAdaptor(namespace, strategy).typeDefinitionRegistry();
        final RuntimeWiring wiring = new RuntimeWiringFactory(database, namespace, strategy).runtimeWiring();

        final SchemaGenerator generator = new SchemaGenerator();
        final GraphQLSchema schema = generator.makeExecutableSchema(registry, wiring);

        return GraphQL.newGraphQL(schema)
                .subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
                .build();
    }
}

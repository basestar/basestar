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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.*;
import io.basestar.auth.Caller;
import io.basestar.database.DatabaseServer;
import io.basestar.database.options.CreateOptions;
import io.basestar.event.EventSerialization;
import io.basestar.graphql.schema.SchemaAdaptor;
import io.basestar.graphql.schema.SchemaConverter;
import io.basestar.graphql.subscription.SubscriberContext;
import io.basestar.graphql.wiring.AnyCoercing;
import io.basestar.graphql.wiring.InterfaceResolver;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.stream.event.SubscriptionPublishEvent;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

class GraphQLTest {

    private Namespace namespace() throws Exception {

        return Namespace.load(GraphQLTest.class.getResource("schema.yml"));
    }

    private GraphQL graphQL(final Namespace namespace) throws Exception {

        final MemoryStorage storage = MemoryStorage.builder().build();
        final DatabaseServer databaseServer = new DatabaseServer(namespace, storage);

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .schema(Name.of("Test4"))
                .id("test4")
                .data(ImmutableMap.of(
                        "test", ImmutableMap.of(
                                "id","test1"
                        ),
                        "test2", ImmutableMap.of(
                                "id","test1"
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .schema(Name.of("Test1"))
                .id("test1")
                .data(ImmutableMap.of(
                        "x", "test1",
                        "z", ImmutableMap.of(
                                "test", ImmutableMap.of(
                                        "id", "test4"
                                )
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .schema(Name.of("Test5"))
                .id("test5")
                .data(ImmutableMap.of(
                        "abstractRef", ImmutableMap.of(
                                "id", "test4"
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .schema(Name.of("Test5"))
                .id("testMissing")
                .data(ImmutableMap.of(
                        "abstractRef", ImmutableMap.of(
                                "id", "missing"
                        )
                ))
                .build()).get();

        return GraphQLAdaptor.builder().database(databaseServer).namespace(namespace).build().graphQL();
    }

    @Test
    void testConvert() throws Exception {

        final SchemaParser parser = new SchemaParser();
        final TypeDefinitionRegistry tdr = parser.parse(GraphQLTest.class.getResourceAsStream("schema.gql"));

        final SchemaConverter converter = new SchemaConverter();
        final Namespace.Builder ns = converter.namespace(tdr);
        assertNotNull(ns);
        ns.yaml(System.out);
    }

    @Test
    void testGet() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final Map<String, Object> get = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("query {\n" +
                        "  readTest1(id:\"test1\") {\n" +
                        "    id\n" +
                        "    z {\n" +
                        "      key\n" +
                        "      value {\n" +
                        "        test {\n" +
                        "          id\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build()).getData();
        assertEquals(Collections.singletonMap(
                "readTest1", ImmutableMap.of(
                        "id", "test1",
                        "z", ImmutableList.of(ImmutableMap.of(
                                "key", "test",
                                "value", ImmutableMap.of("test", ImmutableMap.of(
                                        "id", "test1"
                                )))
                        )
                )
        ), get);
    }

    @Test
    void testGetMissing() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final Map<String, Object> get = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("query {\n" +
                        "  readTest1(id:\"x\") {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();
        assertEquals(Collections.singletonMap(
                "readTest1", null
        ), get);
    }

    @Test
    void testCreate() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final Map<String, Map<String, Object>> create = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  createTest1(id:\"x\", data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "    version\n" +
                        "    created\n" +
                        "    updated\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build()).getData();
        assertEquals(4, create.get("createTest1").size());

        final Map<String, Object> get = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("query {\n" +
                        "  readTest1(id:\"x\") {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build()).getData();
        assertEquals(ImmutableMap.of("readTest1", ImmutableMap.of("id", "x")), get);
    }

    @Test
    void testMultiMutate() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final Map<String, Object> result = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  a: createTest1(id:\"x\", data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "  b: createTest1(id:\"y\", data:{x:\"y\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build()).getData();
        assertEquals(ImmutableMap.of(
                "a", ImmutableMap.of("id", "x"),
                "b", ImmutableMap.of("id", "y")
        ), result);
    }

    @Test
    void testNullErrors() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final Map<String, Object> result = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  a: updateTest1(data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();
    }

    @Test
    void testSubscribeAlias() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final SubscriberContext subscriberContext = mock(SubscriberContext.class);
        when(subscriberContext.subscribe(any(), anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("subscriber", subscriberContext).build())
                .query("subscription {\n" +
                        "  a: subscribeTest1(id:\"x\") {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();

        verify(subscriberContext).subscribe(namespace.requireObjectSchema("Test1"), "x", "a", ImmutableSet.of(Name.of("id")));
    }

    @Test
    void testSubscribeNoAlias() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final SubscriberContext subscriberContext = mock(SubscriberContext.class);
        when(subscriberContext.subscribe(any(), anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("subscriber", subscriberContext).build())
                .query("subscription {\n" +
                        "  subscribeTest1(id:\"x\") {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();

        verify(subscriberContext).subscribe(namespace.requireObjectSchema("Test1"), "x", "subscribeTest1", ImmutableSet.of(Name.of("id")));
    }

    @Test
    void testBatch() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .query("mutation batch($value: String) {\n" +
                        "  batch {\n" +
                        "    a:createTest1(id:\"x\", data:{x:$value}) {\n" +
                        "      id\n" +
                        "    }\n" +
                        "  }\n" +
                        "}")
                .variables(ImmutableMap.of("value", "x"))
                .build());

        assertEquals(ImmutableMap.of("batch", ImmutableMap.of(
                "a", ImmutableMap.of(
                        "id", "x"
                )
        )), result.getData());
        assertEquals(0, result.getErrors().size());
    }

    @Test
    void testExpressionInVariables() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .query("query Assets($filter: String) { queryTest1(query: $filter) { items { id } } }")
                .variables(ImmutableMap.of(
                        "filter", "asset.fileType != 'UNKNOWN' && location.locationType == 'INBOX' && ('2018' IN asset.tags || 'student' IN asset.tags)"
                ))
                .build());

        System.err.println((Object)result.getData());
        System.err.println(result.getErrors());
    }

    @Test
    void testPolymorphic() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .query("query { readTest3(id: \"test4\") { id, ... on Test4 { test2 { x } } } }")
                .build());

        assertEquals(ImmutableMap.of(
                "readTest3", ImmutableMap.of(
                        "id", "test4",
                        "test2", ImmutableMap.of(
                                "x", "test1"
                        )
                )
        ), result.getData());
    }

    @Test
    void testPolymorphicRef() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .query("query { readTest5(id: \"test5\") { id abstractRef { ... on Test4 { test2 { id x } } } } }")
                .build());

        assertEquals(ImmutableMap.of(
                "readTest5", ImmutableMap.of(
                        "id", "test5",
                        "abstractRef", ImmutableMap.of(
                                "test2", ImmutableMap.of(
                                        "id", "test1",
                                        "x", "test1"
                                )
                        )
                )
        ), result.getData());
    }

    @Test
    void testMissingPolymorphicRef() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .query("query { readTest5(id: \"testMissing\") { abstractRef { id __typename } } }")
                .build());

        assertEquals(ImmutableMap.of(
                "readTest5", ImmutableMap.of(
                        "abstractRef", ImmutableMap.of(
                                "id", "missing",
                                "__typename", "Test3__"
                        )
                )
        ), result.getData());
    }

    @Test
    void testReal() {

        final String input = "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";
        final byte[] inputBytes = BaseEncoding.base64().decode(input);
        final SubscriptionPublishEvent event = EventSerialization.gzipBson().deserialize(SubscriptionPublishEvent.class, inputBytes);
        System.err.println(event);
    }

    @Test
    void testGenerator() throws Exception {

        final Namespace namespace = namespace();

        final TypeDefinitionRegistry registry = new SchemaAdaptor(namespace, GraphQLStrategy.DEFAULT).typeDefinitionRegistry();

        final SchemaGenerator generator = new SchemaGenerator();

        final RuntimeWiring.Builder builder = RuntimeWiring.newRuntimeWiring();
        builder.scalar(GraphQLScalarType.newScalar()
                .name(GraphQLStrategy.DEFAULT.anyTypeName())
                .coercing(new AnyCoercing())
                .build());
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof InstanceSchema) {
                if(!((InstanceSchema) schema).isConcrete()) {
                    builder.type(TypeRuntimeWiring.newTypeWiring(GraphQLStrategy.DEFAULT.typeName(schema))
                            .typeResolver(new InterfaceResolver((InstanceSchema)schema, GraphQLStrategy.DEFAULT)));
                }
            }
        });
        final RuntimeWiring wiring = builder.build();

        final GraphQLSchema schema = generator.makeExecutableSchema(registry, wiring);

        final SchemaPrinter printer = new SchemaPrinter();
        System.out.println(printer.print(schema));
    }
}

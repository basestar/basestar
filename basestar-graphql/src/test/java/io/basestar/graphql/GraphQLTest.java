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
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.*;
import io.basestar.auth.Caller;
import io.basestar.database.DatabaseServer;
import io.basestar.database.options.CreateOptions;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.graphql.schema.SchemaAdaptor;
import io.basestar.graphql.schema.SchemaConverter;
import io.basestar.graphql.subscription.SubscriberContext;
import io.basestar.graphql.wiring.InterfaceResolver;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.schema.Property;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.use.UseString;
import io.basestar.secret.SecretContext;
import io.basestar.storage.MemoryStorage;
import io.basestar.util.Name;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@Slf4j
class GraphQLTest {

    private Namespace namespace() throws Exception {

        return Namespace.load(GraphQLTest.class.getResource("schema.yml"));
    }

    private GraphQL graphQL(final Namespace namespace) throws Exception {

        return graphQL(namespace, Emitter.skip());
    }

    private GraphQL graphQL(final Namespace namespace, final Emitter emitter) throws Exception {

        final MemoryStorage storage = MemoryStorage.builder().build();
        final DatabaseServer databaseServer = DatabaseServer.builder().namespace(namespace).storage(storage).emitter(emitter).build();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(Name.of("Test4"))
                .setId("test4")
                .setData(ImmutableMap.of(
                        "test", ImmutableMap.of(
                                "id", "test1"
                        ),
                        "test2", ImmutableMap.of(
                                "id","test1"
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(Name.of("Test1"))
                .setId("test1")
                .setData(ImmutableMap.of(
                        "d", 0.25,
                        "x", "test1",
                        "z", ImmutableMap.of(
                                "test", ImmutableMap.of(
                                        "id", "test4"
                                )
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(Name.of("Test5"))
                .setId("test5")
                .setData(ImmutableMap.of(
                        "abstractRef", ImmutableMap.of(
                                "id", "test4"
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(Name.of("Test5"))
                .setId("testMissing")
                .setData(ImmutableMap.of(
                        "abstractRef", ImmutableMap.of(
                                "id", "missing"
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(Name.of("Test7"))
                .setId("test7")
                .setData(ImmutableMap.of())
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(Name.of("TestAny"))
                .setId("testAny")
                .setData(ImmutableMap.of(
                        "any", ImmutableMap.of(
                                "z", ImmutableList.of(1, 2, 3)
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(Name.of("Point"))
                .setId("point1")
                .setData(ImmutableMap.of(
                        "x", 3,
                        "y", 4
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
                        "    d\n" +
                        "    a { items { schema id test { x } } }\n" +
                        "    b { schema id test { x } }\n" +
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
                        "d", "0.25",
                        "a", ImmutableMap.of(
                                "items", ImmutableList.of(
                                        ImmutableMap.of(
                                                "schema", "Test4",
                                                "id", "test4",
                                                "test", ImmutableMap.of(
                                                        "x", "test1"
                                                )
                                        )
                                )
                        ),
                        "b", ImmutableMap.of(
                                "schema", "Test4",
                                "id", "test4",
                                "test", ImmutableMap.of(
                                        "x", "test1"
                                )
                        ),
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
    void testUpdateCreate() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult update = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  updateTest1(id:\"x\", data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build());
        assertEquals(1, update.getErrors().size());

        final ExecutionResult updateCreate = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  updateTest1(id:\"x\", data:{x:\"x\"}, create:true) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build());

        assertEquals(0, updateCreate.getErrors().size());
        assertEquals(1, updateCreate.<Map<String, Map<String, Object>>>getData().get("updateTest1").size());
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
        when(subscriberContext.subscribe(any(), anyString(), any(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

        graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("subscriber", subscriberContext).build())
                .query("subscription {\n" +
                        "  a: subscribeTest1(id:\"x\") {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();

        verify(subscriberContext).subscribe(namespace.requireObjectSchema("Test1"), "x", "a", ImmutableSet.of(), false);
    }

    @Test
    void testSubscribeNoAlias() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final SubscriberContext subscriberContext = mock(SubscriberContext.class);
        when(subscriberContext.subscribe(any(), anyString(), any(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

        graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("subscriber", subscriberContext).build())
                .query("subscription {\n" +
                        "  subscribeTest1(id:\"x\") {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();

        verify(subscriberContext).subscribe(namespace.requireObjectSchema("Test1"), "x", "subscribeTest1", ImmutableSet.of(), false);
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
                                "__typename", "UnresolvedTest3"
                        )
                )
        ), result.getData());
    }

    @Test
    void testVersionedRef() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  updateTest7(id:\"test7\", data:{\n" +
                        "    versionedRef:{id: \"test7\", version: 1},\n" +
                        "    mapVersionedRef:{key:\"x\", value:{id: \"test7\", version: 1}},\n" +
                        "    arrayVersionedRef:[{id: \"test7\", version: 1}],\n" +
                        "    wrappers:[{ref:{id: \"test7\", version: 1}}]\n" +
                        "  }) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build()).getData();

        final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput()
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .query("query { readTest7(id: \"test7\") { versionedRef { id version schema } " +
                        "mapVersionedRef { key value { id version schema } }" +
                        "arrayVersionedRef { id version schema }" +
                        "wrappers { ref { id version schema } } } }")
                .build());

        assertEquals(ImmutableMap.of(
                "readTest7", ImmutableMap.of(
                        "versionedRef", ImmutableMap.of(
                                "id", "test7",
                                "version", 1,
                                "schema", "Test7"
                        ),
                        "mapVersionedRef", ImmutableList.of(ImmutableMap.of(
                                "key", "x",
                                "value", ImmutableMap.of(
                                        "id", "test7",
                                        "version", 1,
                                        "schema", "Test7"
                                )
                        )),
                        "arrayVersionedRef", ImmutableList.of(ImmutableMap.of(
                                "id", "test7",
                                "version", 1,
                                "schema", "Test7"
                        )),
                        "wrappers", ImmutableList.of(ImmutableMap.of(
                                "ref", ImmutableMap.of(
                                        "id", "test7",
                                        "version", 1,
                                        "schema", "Test7"
                                )
                        ))
                )
        ), result.getData());
    }

    @Test
    void testAny() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult read = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("query { readTestAny(id: \"testAny\") { any } }")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build());

        assertEquals(ImmutableMap.of(
                "readTestAny", ImmutableMap.of(
                        "any", ImmutableMap.of(
                                "z", ImmutableList.of(1, 2, 3)
                        )
                )
        ), read.getData());

        final ExecutionResult update = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation { updateTestAny(id: \"testAny\", data:{ any: {z: true}}) { any } }")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build());

        assertEquals(ImmutableMap.of(
                "updateTestAny", ImmutableMap.of(
                        "any", ImmutableMap.of(
                                "z", true
                        )
                )
        ), update.getData());
    }

    @Test
    void testTransient() throws Exception {

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace);

        final ExecutionResult read = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("query { readPoint(id: \"point1\") { length } }")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build());

        assertEquals(ImmutableMap.of(
                "readPoint", ImmutableMap.of(
                        "length", 5.0
                )
        ), read.getData());
    }

    @Test
    void testGenerator() throws Exception {

        final Namespace namespace = namespace();

        final TypeDefinitionRegistry registry = new SchemaAdaptor(namespace, GraphQLStrategy.DEFAULT).typeDefinitionRegistry();

        final SchemaGenerator generator = new SchemaGenerator();

        final RuntimeWiring.Builder builder = GraphQLAdaptor.runtimeWiringBuilder(GraphQLStrategy.DEFAULT, SecretContext.none());

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

    private void configureMockEmitter(final Emitter emitter) {

        when(emitter.emit(anyCollectionOf(Event.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(emitter.emit(any(Event.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(emitter.emit(anyCollectionOf(Event.class), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(emitter.emit(any(Event.class), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void testEvents() throws Exception {

        final Emitter emitter = mock(Emitter.class);
        configureMockEmitter(emitter);

        final Namespace namespace = namespace();
        final GraphQL graphQL = graphQL(namespace, emitter);

        reset(emitter);
        configureMockEmitter(emitter);

        final Map<String, Map<String, Object>> create = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  createTest1(id:\"x\", data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .context(GraphQLContext.newContext().of("caller", Caller.SUPER).build())
                .build()).getData();
        assertNotNull(create.get("createTest1"));

        verify(emitter, times(1))
                .emit(anyCollectionOf(Event.class));
    }

    @Test
    void testViewOnlyNamespace() {

        final Namespace namespace = Namespace.builder().setSchema("Test", ViewSchema.builder()
                .setProperty("test", Property.builder().setType(UseString.DEFAULT))).build();
        final Emitter emitter = Emitter.skip();
        final MemoryStorage storage = MemoryStorage.builder().build();
        final DatabaseServer databaseServer = DatabaseServer.builder().namespace(namespace).storage(storage).emitter(emitter).build();

        GraphQLAdaptor.builder().database(databaseServer).namespace(namespace).build().graphQL();
    }
}

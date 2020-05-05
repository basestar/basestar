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
import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.basestar.auth.Caller;
import io.basestar.database.DatabaseServer;
import io.basestar.database.options.CreateOptions;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphQLTest {

    private Namespace namespace() throws Exception {

        return Namespace.load(GraphQLTest.class.getResource("schema.yml"));
    }

    private GraphQL graphQL() throws Exception {

        final Namespace namespace = namespace();

        final MemoryStorage storage = MemoryStorage.builder().build();
        final DatabaseServer databaseServer = new DatabaseServer(namespace, storage);

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .schema("Test4")
                .id("test4")
                .data(ImmutableMap.of(
                        "test", ImmutableMap.of(
                                "id","test1"
                        )
                ))
                .build()).get();

        databaseServer.create(Caller.SUPER, CreateOptions.builder()
                .schema("Test1")
                .id("test1")
                .data(ImmutableMap.of(
                        "z", ImmutableMap.of(
                                "test", ImmutableMap.of(
                                        "id", "test4"
                                )
                        )
                ))
                .build()).get();

        return new GraphQLAdaptor(databaseServer, namespace)
                .graphQL();
    }

    @Test
    public void testConvert() throws Exception {

        final SchemaParser parser = new SchemaParser();
        final TypeDefinitionRegistry tdr = parser.parse(GraphQLTest.class.getResourceAsStream("schema.gql"));

        final GraphQLSchemaConverter converter = new GraphQLSchemaConverter();
        final Namespace.Builder ns = converter.namespace(tdr);
        ns.print(System.out);
    }

    @Test
    public void testGet() throws Exception {

        final GraphQL graphQL = graphQL();

        final Map<String, Object> get = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("query {\n" +
                        "  readTest1(id:\"test1\") {\n" +
                        "    id\n" +
                        "    z {\n" +
                        "      __key\n" +
                        "      __value {\n" +
                        "        test {\n" +
                        "          id\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}")
                .build()).getData();
        assertEquals(Collections.singletonMap(
                "readTest1", ImmutableMap.of(
                        "id", "test1",
                        "z", ImmutableList.of(ImmutableMap.of(
                                "__key", "test",
                                "__value", ImmutableMap.of("test", ImmutableMap.of(
                                        "id", "test1"
                                )))
                        )
                )
        ), get);
    }

    @Test
    public void testGetMissing() throws Exception {

        final GraphQL graphQL = graphQL();

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
    public void testCreate() throws Exception {

        final GraphQL graphQL = graphQL();

        final Map<String, Map<String, Object>> create = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  createTest1(id:\"x\", data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "    version\n" +
                        "    created\n" +
                        "    updated\n" +
                        "  }\n" +
                        "}")
                .build()).getData();
        assertEquals(4, create.get("createTest1").size());

        final Map<String, Object> get = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("query {\n" +
                        "  readTest1(id:\"x\") {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();
        assertEquals(ImmutableMap.of("readTest1", ImmutableMap.of("id", "x")), get);

        System.err.println(get);
    }

    @Test
    public void testBatchMutate() throws Exception {

        final GraphQL graphQL = graphQL();

        final Map<String, Object> result = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  a: createTest1(id:\"x\", data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "  b: createTest1(id:\"y\", data:{x:\"y\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();
        assertEquals(ImmutableMap.of(
                "a", ImmutableMap.of("id", "x"),
                "b", ImmutableMap.of("id", "y")
        ), result);
    }

    @Test
    public void testNullErrors() throws Exception {

        final GraphQL graphQL = graphQL();

        final Map<String, Object> result = graphQL.execute(ExecutionInput.newExecutionInput()
                .query("mutation {\n" +
                        "  a: updateTest1(data:{x:\"x\"}) {\n" +
                        "    id\n" +
                        "  }\n" +
                        "}")
                .build()).getData();
    }
}

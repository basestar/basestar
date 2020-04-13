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

import com.google.common.collect.ImmutableMap;
import graphql.GraphQL;
import io.basestar.auth.Caller;
import io.basestar.database.DatabaseServer;
import io.basestar.graphql.wiring.GraphQLFactory;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class GraphQLConverterTest {

    @Test
    public void testConvert() throws IOException, InterruptedException {

        final Namespace namespace = Namespace.load(GraphQLConverterTest.class.getResource("schema.yml"));

        final MemoryStorage storage = MemoryStorage.builder().build();
        final DatabaseServer databaseServer = new DatabaseServer(namespace, storage);

        final Instance ref = databaseServer.create(Caller.SUPER, "Test4", ImmutableMap.of()).join();

        final Instance instance = databaseServer.create(Caller.SUPER, "Test1", ImmutableMap.of(
                "z", ImmutableMap.of(
                        "a", ref
                )
        )).join();

        databaseServer.create(Caller.SUPER, "Test4", ImmutableMap.of(
                "test", ImmutableMap.of(
                        "id", instance.getId()
                )
        )).join();

        final GraphQL graphql = new GraphQLFactory(databaseServer, namespace).graphQL();
//
//        final API api = new GraphQLAPI(graphql);
//
//        final UndertowConnector connector = new UndertowConnector(api, "localhost", 8000);
//        connector.start();
//        Thread.sleep(10000000);
////        connector.stop();


//        schema.getQueryType().

//        final SchemaPrinter printer = new SchemaPrinter();
//        System.err.println(printer.print(schema));
    }
}

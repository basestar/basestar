package io.basestar.mapper;

/*-
 * #%L
 * basestar-mapper
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
import io.basestar.mapper.annotation.*;
import io.basestar.mapper.internal.TypeMapper;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.schema.jsr380.Assert;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;
import lombok.Data;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class TestMappingContext {

    @Data
    @ObjectSchema
    public static class Post {

        @Id
        @Nullable
        private String id;

        @Nullable
        @Link(expression = "target.id == this.id", sort = "blah:desc")
        private List<Comment> comments;

        @Created
        @Nullable
        private LocalDateTime created;

        @Updated
        @Nullable
        private LocalDateTime updated;

        @Hash
        @Nullable
        private String hash;

        @Nullable
        @Size(min = 10, max = 100)
        private double value;

        @Version
        @Nullable
        @Assert("x == 1")
        private Long version;

        @Nullable
        @Property
        private String test;

        @Nullable
        private LocalDate date;

        @Nullable
        private Comment comment;

        @ObjectSchema
        @Index(name = "parent", partition = "parent.id")
        @Index(name = "parent", partition = "parent.id")
        @Index(name = "parent", partition = "parent.id")
        public static class Comment {

            @Id
            private String id;

            @Nullable
            private Comment comment;

            @Nullable
            @Link(expression = "target.id == this.id")
            private List<Comment> comments;
        }
    }

    @ViewSchema
    @From(Post.class)
    @Where("x == 1")
    public static class PostView {

    }

    @Test
    public void testCreateSchema() {

        final MappingContext mappingContext = new MappingContext();

        final SchemaMapper<Post, Instance> schemaMapper = mappingContext.schemaMapper(Post.class);
        final Schema.Builder<?> schema = schemaMapper.schemaBuilder();

        final Post post = schemaMapper.marshall(new Instance(ImmutableMap.of(
                io.basestar.schema.ObjectSchema.ID, "test",
//                Reserved.VERSION, 1L,
                "date", "2020-01-01",
                "comments", ImmutableList.of(ImmutableMap.of(
                        io.basestar.schema.ObjectSchema.ID, "c1"
                )),
                "comment", ImmutableMap.of(
                        io.basestar.schema.ObjectSchema.ID, "c1"
                ),
                "value", (byte) 12
        )));

        System.err.println(schema);
        System.err.println(post);

        final Map<String, ?> unmarshalled = schemaMapper.unmarshall(post);

        System.err.println(unmarshalled);

        final Namespace.Builder ns = mappingContext.namespace(Post.class);

        System.err.println(ns);
    }

    @Test
    public void testViewSchema() {

        final MappingContext mappingContext = new MappingContext();

        final SchemaMapper<PostView, Instance> schemaMapper = mappingContext.schemaMapper(PostView.class);
        final Schema.Builder<?> schema = schemaMapper.schemaBuilder();

        System.err.println(schema);

    }

    @Test
    public void testSchemaOfSchema() throws IOException {

        final MappingContext mappingContext = new MappingContext(new MappingStrategy.Default() {
            @Override
            public Name schemaName(final MappingContext context, final TypeContext type) {

                final String simpleName = type.simpleName();
                if("Descriptor".equals(type.simpleName())) {
                    return Name.of(type.enclosing().simpleName());
                } else {
                    return Name.of(simpleName);
                }
            }

            @Override
            public TypeMapper typeMapper(final MappingContext context, final TypeContext type) {

                if(type.simpleName().equals("Name")) {
                    return new TypeMapper.OfString();
                } else {
                    return TypeMapper.fromDefault(context, type);
                }
            }
        });

        final Namespace.Builder builder = mappingContext.namespace(
                io.basestar.schema.ObjectSchema.Descriptor.class,
                io.basestar.schema.StructSchema.Descriptor.class,
                io.basestar.schema.EnumSchema.Descriptor.class,
                io.basestar.schema.ViewSchema.Descriptor.class
        );

        builder.yaml(System.out);
    }
}

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
import io.basestar.mapper.internal.SchemaMapper;
import io.basestar.schema.Instance;
import io.basestar.schema.Reserved;
import io.basestar.schema.Schema;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class TestMapper {

    @Data
    @ObjectSchema
    public static class Post {

        @Id
        private String id;

        @Link(expression = "target.id == this.id", sort = "blah:desc")
        private List<Comment> comments;

        @Created
        private LocalDateTime created;

        @Updated
        private LocalDateTime updated;

        @Hash
        private String hash;

        @Version
        private Long version;

        @Property
        private String test;

        private LocalDate date;

        private Comment comment;

        @ObjectSchema
        @Index(name = "parent", partition = "parent.id")
        public static class Comment {

            @Id
            private String id;
        }
    }

    @Test
    public void testCreateSchema() {

        final Mapper mapper = new Mapper();

        final SchemaMapper<Post, Instance> schemaMapper = mapper.schema(Post.class);
        final Schema.Builder<?> schema = schemaMapper.schema();

        final Post post = schemaMapper.marshall(new Instance(ImmutableMap.of(
                Reserved.ID, "test",
                Reserved.VERSION, 1L,
                "date", "2020-01-01",
                "comments", ImmutableList.of(ImmutableMap.of(
                        Reserved.ID, "c1"
                )),
                "comment", ImmutableMap.of(
                        Reserved.ID, "c1"
                )
        )));

        System.err.println(schema);
        System.err.println(post);
    }
}

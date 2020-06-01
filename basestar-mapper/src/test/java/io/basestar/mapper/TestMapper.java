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

import io.basestar.mapper.annotation.*;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

public class TestMapper {

    @ObjectSchema
    public static class Post {

        @Id
        private String id;

        @Link(expression = "target.id == this.id", sort = "blah:desc")
        private List<TestMapper> comments;

        @Created
        private LocalDateTime created;

        @Updated
        private LocalDateTime updated;

        @Hash
        private String hash;

        @Property
        private String test;

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
        System.err.println(mapper.schema(Post.class).toString());
    }
}

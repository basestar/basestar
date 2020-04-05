package io.basestar.mapper;

import io.basestar.mapper.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@ObjectSchema
public class Test {

    @Id
    private String id;

    @Link(query = "target.id == this.id", sort = "blah:desc")
    private List<Test> comments;

    @Created
    private LocalDateTime created;

    @Updated
    private LocalDateTime updated;

    @Hash
    private String hash;

    @ObjectSchema
    @Index(name = "parent", partition = "parent.id")
    public static class Comment {

        @Id
        private String id;

//        @Reference
        private Test parent;
    }

    public static void main(final String[] args) {

        final Mapper mapper = new Mapper();
        System.err.println(mapper.schema(Test.class));
    }
}

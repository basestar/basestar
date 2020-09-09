package io.basestar.maven.test.existing;

import io.basestar.mapper.annotation.ObjectSchema;
import lombok.Data;

@Data
@ObjectSchema(name = "existing.Test")
public class Test {

    private String a;
}

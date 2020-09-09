package io.basestar.schema.jsr380;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Assert.List.class)
@Constraint(validatedBy = AssertConstraintValidator.class)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE, ElementType.TYPE_USE})
public @interface Assert {

    String value();

    String message() default "{io.basestar.schema.jsr380.Assert.message}";

    Class<?>[] groups() default { };

    @SuppressWarnings("unused")
    Class<? extends Payload>[] payload() default { };

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
    @interface List {

        Assert[] value();
    }
}

package io.basestar.schema.jsr380;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(When.List.class)
@Constraint(validatedBy = WhenConstraintValidator.class)
@Target({ElementType.TYPE})
public @interface When {

    String[] conditions();

    Class<?> enable();

    String message() default "{io.basestar.schema.jsr380.When.message}";

    Class<?>[] groups() default { };

    @SuppressWarnings("unused")
    Class<? extends Payload>[] payload() default { };

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    @interface List {

        When[] value();
    }
}

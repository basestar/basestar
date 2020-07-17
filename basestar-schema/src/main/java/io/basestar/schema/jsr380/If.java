package io.basestar.schema.jsr380;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(If.List.class)
@Constraint(validatedBy = IfConstraintValidator.class)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE, ElementType.TYPE_USE})
public @interface If {

    String value();

    String message() default "{io.basestar.schema.jsr380.If.message}";

    Class<?>[] groups() default { };

    Class<? extends Payload>[] payload() default { };

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
    @interface List {

        If[] value();
    }
}

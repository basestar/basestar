package io.basestar.test.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * Mark test-only classes so that they are ignored for coverage
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({PACKAGE, TYPE, ANNOTATION_TYPE})
public @interface PretendGenerated {

}

package io.basestar.expression;

public interface Matcher<R> {

    R match(final Expression e);
}

package com.indeed.squall.iql2.language;

import com.google.common.base.Objects;
import org.antlr.v4.runtime.ParserRuleContext;

public class Positioned<T> extends AbstractPositional implements Positional {
    private final T t;

    private Positioned(T t) {
        this.t = t;
    }

    public T unwrap() {
        return t;
    }

    public static <T> Positioned<T> unpositioned(T t) {
        return new Positioned<>(t);
    }

    public static <T> Positioned<T> from(T t, Positional positional) {
        final Positioned<T> positioned = new Positioned<>(t);
        positioned.copyPosition(positional);
        return positioned;
    }

    public static <T> Positioned<T> from(T t, ParserRuleContext parserRuleContext) {
        final Positioned<T> positioned = new Positioned<>(t);
        positioned.copyPosition(parserRuleContext);
        return positioned;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Positioned<?> that = (Positioned<?>) o;
        return Objects.equal(t, that.t);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(t);
    }

    @Override
    public String toString() {
        return "Positioned{" +
                "t=" + t +
                '}';
    }
}

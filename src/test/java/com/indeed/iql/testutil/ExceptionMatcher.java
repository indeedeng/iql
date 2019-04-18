package com.indeed.iql.testutil;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.List;

public class ExceptionMatcher<T extends Throwable> extends TypeSafeMatcher<Throwable> {
    private final List<Matcher<? super Throwable>> exceptionMatchers = new ArrayList<>();
    private final List<Matcher<? super String>> messageMatchers = new ArrayList<>();

    public static <T extends Throwable> ExceptionMatcher<T> withType(final Class<T> clazz) {
        return new ExceptionMatcher<T>().withException(Matchers.isA(clazz));
    }

    private ExceptionMatcher() {
    }

    public ExceptionMatcher<T> withException(final Matcher<? super T> matcher) {
        exceptionMatchers.add(new TypeSafeMatcher<Throwable>() {
            @Override
            protected boolean matchesSafely(final Throwable throwable) {
                return matcher.matches(throwable);
            }

            @Override
            public void describeTo(final Description description) {
                matcher.describeTo(description);
            }
        });
        return this;
    }

    public ExceptionMatcher<T> withMessage(final Matcher<? super String> matcher) {
        messageMatchers.add(matcher);
        return this;
    }

    private Matcher<Throwable> getExceptionMatcher() {
        return Matchers.allOf(exceptionMatchers);
    }

    private Matcher<String> getMessageMatcher() {
        return Matchers.allOf(messageMatchers);
    }

    @Override
    protected boolean matchesSafely(final Throwable t) {
        return getExceptionMatcher().matches(t) && getMessageMatcher().matches(t.getMessage());
    }

    @Override
    public void describeTo(final Description description) {
        description
                .appendText("exception that ")
                .appendDescriptionOf(getExceptionMatcher())
                .appendText(" with message ")
                .appendDescriptionOf(getMessageMatcher());
    }
}

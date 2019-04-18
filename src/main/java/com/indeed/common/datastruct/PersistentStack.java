package com.indeed.common.datastruct;

import lombok.Value;

import javax.annotation.Nullable;
import java.util.Objects;

public class PersistentStack<E> {
    @Nullable // Null means it's a sentinel
    private final Node<E> node;

    private PersistentStack() {
        this(null);
    }

    private PersistentStack(final E element, final PersistentStack<E> next) {
        this(new Node<>(element, next));
    }

    private PersistentStack(@Nullable final Node<E> node) {
        this.node = node;
    }

    public static <E> PersistentStack<E> empty() {
        return new PersistentStack<>();
    }

    public boolean isEmpty() {
        return node == null;
    }

    public E top() {
        if (this.isEmpty()) {
            throw new IllegalArgumentException("Can't get top from empty stack");
        }
        return Objects.requireNonNull(this.node).element;
    }

    public PersistentStack<E> pushed(final E element) {
        return new PersistentStack<>(element, this);
    }

    public PersistentStack<E> popped() {
        if (this.isEmpty()) {
            throw new IllegalArgumentException("Can't pop from empty stack");
        }
        return Objects.requireNonNull(this.node).next;
    }

    @Value
    private static class Node<E> {
        final E element;
        @Nullable
        final PersistentStack<E> next;
    }
}

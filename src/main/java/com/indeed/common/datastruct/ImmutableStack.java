package com.indeed.common.datastruct;

import lombok.Value;

import javax.annotation.Nullable;
import java.util.Objects;

public class ImmutableStack<E> {
    @Nullable // Null means it's a sentinel
    private final Node<E> node;

    private ImmutableStack() {
        this(null);
    }

    private ImmutableStack(final E element, final ImmutableStack<E> next) {
        this(new Node<>(element, next));
    }

    private ImmutableStack(@Nullable final Node<E> node) {
        this.node = node;
    }

    public static <E> ImmutableStack<E> empty() {
        return new ImmutableStack<>();
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

    public ImmutableStack<E> pushed(final E element) {
        return new ImmutableStack<>(element, this);
    }

    public ImmutableStack<E> popped() {
        if (this.isEmpty()) {
            throw new IllegalArgumentException("Can't pop from empty stack");
        }
        return Objects.requireNonNull(this.node).next;
    }

    @Value
    private static class Node<E> {
        final E element;
        @Nullable
        final ImmutableStack<E> next;
    }
}

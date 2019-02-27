/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.iql1.ez;

import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * @author jplaisance
 */
public final class GroupKey<E extends Comparable> {
    @Nullable
    private final List<E> front;
    @Nullable
    private final List<E> back;

    private static final GroupKey EMPTY = new GroupKey(null, null);

    public static <E extends Comparable> GroupKey<E> empty() {
        return EMPTY;
    }

    public static <E extends Comparable> GroupKey<E> singleton(E e) {
        return EMPTY.add(e);
    }

    private GroupKey(@Nullable final List<E> front, @Nullable final List<E> back) {
        this.front = front;
        this.back = back;
    }

    private static final class List<E> {
        private final E head;

        @Nullable
        private final List<E> tail;

        @Nullable
        private final E last;

        private List(final E head, @Nullable final List<E> tail) {
            this.head = head;
            this.tail = tail;
            this.last = (tail==null)? head : tail.last;
        }

        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final List list = (List) o;

            return Objects.equals(head, list.head) && Objects.equals(tail, list.tail);
        }

        public int hashCode() {
            int result = head != null ? head.hashCode() : 0;
            result = 31 * result + (tail != null ? tail.hashCode() : 0);
            return result;
        }

        public E getLast() {
            return last;
        }
    }

    public E head() {
        if (front == null) {
            if (back == null) {
                throw new IllegalStateException("empty key has no head");
            }
            return back.last;
        }
        return front.head;
    }

    public E getLastInserted() {
        if (back == null) {
            if (front == null) {
                throw new IllegalStateException("Key is empty");
            }
            return front.last;
        }
        return back.head;
    }

    public GroupKey<E> tail() {
        if (front == null) {
            if (back == null) {
                throw new IllegalStateException("empty key has no tail");
            }
            List<E> reversed = null;
            List<E> current = back;
            while (current.tail != null) {
                reversed = new List<>(current.head, reversed);
                current = current.tail;
            }
            return new GroupKey<>(reversed, null);
        }
        return new GroupKey<>(front.tail, back);
    }

    public GroupKey<E> add(E e) {
        return new GroupKey<>(front, new List<>(e, back));
    }

    public boolean isEmpty() {
        return front == null && back == null;
    }

    public String toString() {
        return Lists.newArrayList(this).toString();
    }

    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GroupKey groupKey = (GroupKey) o;

        return Objects.equals(back, groupKey.back) && Objects.equals(front, groupKey.front);
    }

    public int hashCode() {
        int result = front != null ? front.hashCode() : 0;
        result = 31 * result + (back != null ? back.hashCode() : 0);
        return result;
    }


}

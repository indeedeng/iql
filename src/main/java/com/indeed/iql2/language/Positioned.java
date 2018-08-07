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

package com.indeed.iql2.language;

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

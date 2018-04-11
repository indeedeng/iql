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

package com.indeed.squall.iql2.language;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

public class UpperCaseInputStream implements CharStream {
    private final CharStream charStream;

    public UpperCaseInputStream(CharStream charStream) {
        this.charStream = charStream;
    }

    @Override
    public String getText(Interval interval) {
        return charStream.getText(interval);
    }

    @Override
    public void consume() {
        charStream.consume();
    }

    @Override
    public int LA(int i) {
        final int la = charStream.LA(i);
        if (la <= 0) {
            return la;
        } else {
            return Character.toUpperCase(la);
        }
    }

    @Override
    public int mark() {
        return charStream.mark();
    }

    @Override
    public void release(int i) {
        charStream.release(i);
    }

    @Override
    public int index() {
        return charStream.index();
    }

    @Override
    public void seek(int i) {
        charStream.seek(i);
    }

    @Override
    public int size() {
        return charStream.size();
    }

    @Override
    public String getSourceName() {
        return charStream.getSourceName();
    }

    @Override
    public String toString() {
        return "UpperCaseInputStream{" +
                "charStream=" + charStream +
                '}';
    }
}

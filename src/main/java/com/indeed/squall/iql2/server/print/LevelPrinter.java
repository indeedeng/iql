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

package com.indeed.squall.iql2.server.print;

public class LevelPrinter {
    private static int INDENT_LENGTH = 4;
    private StringBuilder sb;
    private int depth;

    public LevelPrinter() {
        sb = new StringBuilder();
    }

    public void push(String s) {
        depth++;
        for (int i = 0; i < depth*INDENT_LENGTH; i++) {
            sb.append(' ');
        }
        sb.append(s).append('\n');
    }

    public void pop() {
        if (--depth < 0) {
            throw new IllegalAccessError("no more level to pop");
        }
    }


    @Override
    public String toString() {
        if (depth != 0) {
            throw  new IllegalAccessError(String.format("remaning %d levels to pop", depth));
        }
        return sb.toString();
    }
}
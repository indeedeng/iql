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

import org.antlr.v4.runtime.Token;

public class Position {
    public final int line;
    public final int character;
    public final int startIndex;
    public final int stopIndex;

    public Position(int line, int character, int startIndex, int stopIndex) {
        this.line = line;
        this.character = character;
        this.startIndex = startIndex;
        this.stopIndex = stopIndex;
    }

    public static Position from(Token token) {
        return new Position(token.getLine(), token.getCharPositionInLine(), token.getStartIndex(), token.getStopIndex());
    }
}

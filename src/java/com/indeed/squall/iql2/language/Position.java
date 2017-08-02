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

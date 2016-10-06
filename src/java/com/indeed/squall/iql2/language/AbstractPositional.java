package com.indeed.squall.iql2.language;

import org.antlr.v4.runtime.ParserRuleContext;

public abstract class AbstractPositional implements Positional {
    private Position start;
    private Position end;

    public AbstractPositional() {
    }

    protected AbstractPositional(Position start, Position end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public Position getStart() {
        return start;
    }

    @Override
    public Position getEnd() {
        return end;
    }

    public void setPosition(Position start, Position end) {
        this.start = start;
        this.end = end;
    }

    public void copyPosition(ParserRuleContext parserRuleContext) {
        this.setPosition(Position.from(parserRuleContext.start), Position.from(parserRuleContext.stop));
    }

    public void copyPosition(Positional positional) {
        this.setPosition(positional.getStart(), positional.getEnd());
    }
}

package com.indeed.squall.iql2.language;

import org.antlr.v4.runtime.ParserRuleContext;

public abstract class AbstractPositional implements Positional {
    // TODO: Change this to be an Interval.
    private Position start;
    private Position end;
    private ParserRuleContext parserRuleContext = null;

    public AbstractPositional() {
    }

    @Override
    public Position getStart() {
        return start;
    }

    @Override
    public Position getEnd() {
        return end;
    }

    @Override
    public ParserRuleContext getParserRuleContext() {
        return parserRuleContext;
    }

    public void setPosition(Position start, Position end) {
        this.start = start;
        this.end = end;
    }

    public void copyPosition(ParserRuleContext parserRuleContext) {
        this.setPosition(Position.from(parserRuleContext.start), Position.from(parserRuleContext.stop));
        this.parserRuleContext = parserRuleContext;
    }

    public void copyPosition(Positional positional) {
        this.setPosition(positional.getStart(), positional.getEnd());
    }
}

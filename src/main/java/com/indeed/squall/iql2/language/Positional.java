package com.indeed.squall.iql2.language;

import org.antlr.v4.runtime.ParserRuleContext;

public interface Positional {
    Position getStart();

    Position getEnd();

    ParserRuleContext getParserRuleContext();
}

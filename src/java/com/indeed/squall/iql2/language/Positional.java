package com.indeed.squall.iql2.language;

import org.antlr.v4.runtime.misc.Interval;

public interface Positional {
    Position getStart();

    Position getEnd();

    Interval getCommentBefore();

    Interval getCommentAfter();
}

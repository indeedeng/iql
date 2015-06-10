package com.indeed.jql.language;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

public class LowerCaseInputStream implements CharStream {
    private final CharStream charStream;

    public LowerCaseInputStream(CharStream charStream) {
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
            return Character.toLowerCase(la);
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
}

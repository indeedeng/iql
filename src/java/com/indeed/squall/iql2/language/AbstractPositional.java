package com.indeed.squall.iql2.language;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

public abstract class AbstractPositional implements Positional {
    // TODO: Change this to be an Interval.
    private Position start;
    private Position end;

    private Interval commentBefore = null;
    private Interval commentAfter = null;

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
    public Interval getCommentBefore() {
        return commentBefore;
    }

    @Override
    public Interval getCommentAfter() {
        return commentAfter;
    }

    public void setPosition(Position start, Position end) {
        this.start = start;
        this.end = end;
    }

    public void setCommentBefore(Interval commentBefore) {
        this.commentBefore = commentBefore;
    }

    public void setCommentAfter(Interval commentAfter) {
        this.commentAfter = commentAfter;
    }

    public void copyPosition(ParserRuleContext parserRuleContext) {
        this.setPosition(Position.from(parserRuleContext.start), Position.from(parserRuleContext.stop));

        // Find the closest node to the right. May not share a direct parent.
        if (parserRuleContext.getParent() != null) {
            ParserRuleContext cur = parserRuleContext;
            while (cur.getParent() != null && cur.getParent().getChildCount() != 0 && cur.getParent().children.indexOf(cur) == cur.getParent().getChildCount() - 1) {
                cur = cur.getParent();
            }
            if (cur.getParent() != null && cur.getParent().children.indexOf(cur) != cur.getParent().getChildCount() - 1) {
                final ParseTree nextChild = cur.getParent().getChild(cur.getParent().children.indexOf(cur) + 1);
                final int thisStop = parserRuleContext.stop.getStopIndex();
                final int nextStart;
                if (nextChild instanceof ParserRuleContext) {
                    nextStart = ((ParserRuleContext) nextChild).start.getStartIndex();
                } else if (nextChild instanceof TerminalNode) {
                    nextStart = ((TerminalNode) nextChild).getSymbol().getStartIndex();
                } else {
                    nextStart = -1;
                }
                if (nextStart != -1) {
                    if (thisStop + 1 < nextStart - 1) {
                        this.setCommentAfter(Interval.of(thisStop + 1, nextStart - 1));
                    }
                }
            }
        }

        // Find the closest node to the left. May not share a direct parent.
        if (parserRuleContext.getParent() != null) {
            ParserRuleContext cur = parserRuleContext;
            while (cur.getParent() != null && cur.getParent().getChildCount() != 0 && cur.getParent().children.indexOf(cur) == 0) {
                cur = cur.getParent();
            }
            if (cur.getParent() != null && cur.getParent().children.indexOf(cur) != 0) {
                final ParseTree prevChild = cur.getParent().getChild(cur.getParent().children.indexOf(cur) - 1);
                final int thisStart = parserRuleContext.start.getStartIndex();
                final int prevStop;
                if (prevChild instanceof ParserRuleContext) {
                    prevStop = ((ParserRuleContext) prevChild).stop.getStopIndex();
                } else if (prevChild instanceof TerminalNode) {
                    prevStop = ((TerminalNode) prevChild).getSymbol().getStopIndex();
                } else {
                    prevStop = -1;
                }

                if (prevStop != -1) {
                    if (prevStop + 1 < thisStart - 1) {
                        this.setCommentBefore(Interval.of(prevStop + 1, thisStart - 1));
                    }
                }
            }
        }
    }

    public void copyPosition(Positional positional) {
        this.setPosition(positional.getStart(), positional.getEnd());
        this.setCommentBefore(positional.getCommentBefore());
        this.setCommentAfter(positional.getCommentAfter());
    }
}

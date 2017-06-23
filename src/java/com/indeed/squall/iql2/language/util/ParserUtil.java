package com.indeed.squall.iql2.language.util;

import com.google.common.base.Optional;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 *
 */
public class ParserUtil {

    public static Optional<Interval> getNextNode(ParserRuleContext parserRuleContext) {
        if (parserRuleContext != null && parserRuleContext.getParent() != null) {
            ParserRuleContext cur = parserRuleContext;
            while ((cur.getParent() != null) && (cur.getParent().getChildCount() != 0) &&
                    (cur.getParent().children.indexOf(cur) == (cur.getParent().getChildCount() - 1))) {
                cur = cur.getParent();
            }
            if ((cur.getParent() != null) && (cur.getParent().children.indexOf(cur) != (cur.getParent().getChildCount() - 1))) {
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
                        return Optional.of(Interval.of(thisStop + 1, nextStart - 1));
                    }
                }
            }
        }
            return Optional.absent();
    }

    public static Optional<Interval> getPreviousNode(ParserRuleContext parserRuleContext) {
        if (parserRuleContext != null && parserRuleContext.getParent() != null) {
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
                        return Optional.of(Interval.of(prevStop + 1, thisStart - 1));
                    }
                }
            }
        }
        return Optional.absent();
    }
}

package com.indeed.imhotep.ez;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import com.indeed.imhotep.api.FTGSIterator;

/**
 * Allows iterating over return values of the provided callback which is being run on each field/term/group tuple.
 * @author vladimir
 */
public class FTGSCallbackIterator<E> extends AbstractIterator<E> implements PeekingIterator<E> {
    // current FTGS iteration state cache
    String field;
    boolean isIntField;
    long termInt;
    String termStr;

    // flags for whether we need to advance field/term
    boolean fieldOver = true;
    boolean termOver = true;

    private final EZImhotepSession.FTGSIteratingCallback<E> callback;
    private final FTGSIterator ftgsIterator;

    public FTGSCallbackIterator(EZImhotepSession.FTGSIteratingCallback<E> callback, FTGSIterator ftgsIterator) {
        this.callback = callback;
        this.ftgsIterator = ftgsIterator;
    }


    @Override
    protected E computeNext() {
        while (!fieldOver || ftgsIterator.nextField()) {
            if(fieldOver) {
                field = ftgsIterator.fieldName();
                isIntField = ftgsIterator.fieldIsIntType();
                fieldOver = false;
            }
            while (!termOver || ftgsIterator.nextTerm()) {
                if(termOver) {
                    if(isIntField) {
                        termInt = ftgsIterator.termIntVal();
                    } else {
                        termStr = ftgsIterator.termStringVal();
                    }
                    termOver = false;
                }
                if (ftgsIterator.nextGroup()) {
                    final int group = ftgsIterator.group();
                    ftgsIterator.groupStats(callback.stats);
                    if (isIntField) {
                        return callback.intTermGroup(field, termInt, group);
                    } else {
                        return callback.stringTermGroup(field, termStr, group);
                    }
                }
                termOver = true;
            }
            fieldOver = true;
        }
        return endOfData();
    }
}

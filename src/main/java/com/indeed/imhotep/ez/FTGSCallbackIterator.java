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
 package com.indeed.imhotep.ez;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Allows iterating over return values of the provided callback which is being run on each field/term/group tuple.
 * @author vladimir
 */
public class FTGSCallbackIterator<E> extends AbstractIterator<E> implements PeekingIterator<E>, Closeable {
    private static final Logger log = Logger.getLogger(FTGSCallbackIterator.class);

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

    @Override
    public void close() throws IOException {
        Closeables2.closeQuietly(ftgsIterator, log);
    }
}

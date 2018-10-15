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

package com.indeed.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import java.util.function.Consumer;;
import com.indeed.iql2.execution.groupkeys.sets.RandomGroupKeySet;
import com.indeed.util.core.TreeTimer;

import java.io.IOException;

/**
 *
 */
public class ExplodeRandom implements Command {
    private final String field;
    private final int k;
    private final String salt;

    public ExplodeRandom(String field, int k, String salt) {
        this.field = field;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final boolean isIntField = session.isIntField(field);
        final int numGroups = session.numGroups;
        if (numGroups != 1) {
            throw new IqlKnownException.ExecutionException("Can only use RANDOM() regroup as first GROUP BY");
        }
        final double[] percentages = new double[k - 1];
        final int[] resultGroups = new int[k];
        for (int i = 0; i < k - 1; i++) {
            final double end = ((double)(i + 1)) / k;
            percentages[i] = end;
            resultGroups[i] = i + 2;
        }
        resultGroups[k - 1] = k + 1;
        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                timer.push("randomMultiRegroup");
                session.randomMultiRegroup(field, isIntField, salt, 1, percentages, resultGroups);
                timer.pop();
            }
        });

        session.assumeDense(new RandomGroupKeySet(session.groupKeySet, k + 1));
    }
}

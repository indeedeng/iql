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
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.iql2.execution.groupkeys.sets.RandomGroupKeySet;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.logging.TracingTreeTimer;

/**
 *
 */
public class ExplodeRandom implements Command {
    private final FieldSet field;
    private final int k;
    private final String salt;

    public ExplodeRandom(FieldSet field, int k, String salt) {
        this.field = field;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        final boolean isIntField = session.isIntField(field);

        final DocMetric.Random randomDocMetric = new DocMetric.Random(field, isIntField, k, salt);

        session.process(new SessionCallback() {
            @Override
            public void handle(final TracingTreeTimer timer, final String name, final ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                timer.push("pushStats");
                session.pushStats(randomDocMetric.getPushes(session.getDatasetName()));
                timer.pop();

                timer.push("metricRegroup");
                session.metricRegroup(0, 0, k + 1, 1, true);
                timer.pop();

                timer.push("popStat");
                session.popStat();
                timer.pop();
            }
        });

        session.assumeDense(new RandomGroupKeySet(session.groupKeySet, k + 1, session.formatter));
    }
}

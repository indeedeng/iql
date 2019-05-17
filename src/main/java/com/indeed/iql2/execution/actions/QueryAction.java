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

package com.indeed.iql2.execution.actions;

import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.util.logging.TracingTreeTimer;

import java.util.Map;

public class QueryAction implements Action {
    public final Map<String, Query> perDatasetQuery;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public QueryAction(final Map<String, Query> perDatasetQuery, final int targetGroup, final int positiveGroup, final int negativeGroup) {
        this.perDatasetQuery = perDatasetQuery;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(final Session session) throws ImhotepOutOfMemoryException {
        session.process(new SessionCallback() {
            @Override
            public void handle(final TracingTreeTimer timer, final String name, final ImhotepSession session) throws ImhotepOutOfMemoryException {
                if (!perDatasetQuery.containsKey(name)) {
                    return;
                }

                timer.push("regroup");
                final Query query = perDatasetQuery.get(name);
                session.regroup(new QueryRemapRule(targetGroup, query, negativeGroup, positiveGroup));
                timer.pop();
            }
        });
    }

    @Override
    public String toString() {
        return "QueryAction{" +
                "perDatasetQuery=" + perDatasetQuery +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}

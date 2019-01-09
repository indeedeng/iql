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

import com.google.common.primitives.Doubles;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GetFieldMin implements IterateHandlerable<double[]>, Command {
    private static final Logger log = Logger.getLogger(GetFieldMin.class);

    public final FieldSet field;

    public GetFieldMin(FieldSet field) {
        this.field = field;
    }

    @Override
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public double[] evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        return IterateHandlers.executeSingle(session, field, iterateHandler(session));
    }

    @Override
    public IterateHandler<double[]> iterateHandler(final Session session) {
        return new IterateHandlerImpl(session.numGroups);
    }

    private class IterateHandlerImpl implements IterateHandler<double[]> {
        private double[] min;

        public IterateHandlerImpl(int numGroups) {
            min = new double[numGroups + 1];
            Arrays.fill(min, Double.POSITIVE_INFINITY);
        }

        @Override
        public Set<String> scope() {
            return field.datasets();
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new Session.IntIterateCallback() {
                @Override
                public void term(final long term, final long[] stats, final int group) {
                    min[group] = Math.min(min[group], term);
                }

                @Override
                public boolean needSorted() {
                    return false;
                }

                @Override
                public boolean needGroup() {
                    return true;
                }

                @Override
                public boolean needStats() {
                    return false;
                }
            };
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            return new Session.StringIterateCallback() {
                @Override
                public void term(final String term, final long[] stats, final int group) {
                    try {
                        final long v = Long.parseLong(term);
                        min[group] = Math.min(min[group], v);
                    } catch (NumberFormatException ignored) {
                    }
                }

                @Override
                public boolean needSorted() {
                    return false;
                }

                @Override
                public boolean needGroup() {
                    return true;
                }

                @Override
                public boolean needStats() {
                    return false;
                }
            };
        }

        @Override
        public double[] finish() {
            for (int i = 0; i < min.length; i++) {
                min[i] = (min[i] == Double.POSITIVE_INFINITY) ? Double.NaN : min[i];
            }
            return min;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
        }
    }
}

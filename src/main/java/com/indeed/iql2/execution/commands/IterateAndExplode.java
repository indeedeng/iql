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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.TermSelects;
import com.indeed.iql2.execution.commands.misc.FieldIterateOpts;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class IterateAndExplode implements Command {
    public final String field;
    public final List<AggregateMetric> selecting;
    public final FieldIterateOpts fieldOpts;
    public final Optional<String> explodeDefaultName;
    @Nullable
    public final Set<String> scope;

    // TODO: Null is horrible, put on some sort of options object
    public IterateAndExplode(String field, List<AggregateMetric> selecting, FieldIterateOpts fieldOpts, Optional<String> explodeDefaultName, @Nullable Set<String> scope) {
        this.field = field;
        this.selecting = selecting;
        this.fieldOpts = fieldOpts;
        this.explodeDefaultName = explodeDefaultName;
        this.scope = scope == null ? null : ImmutableSet.copyOf(scope);
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException, IOException {
        final boolean isIntField = session.isIntField(field);
        final TermsCollector terms = new TermsCollector(isIntField, session.numGroups);
        SimpleIterate.evaluate(session, field, selecting, fieldOpts, scope, terms);
        // TODO: change all Optional to java.util.Optional
        final java.util.Optional<String> defaultName =
                explodeDefaultName.isPresent() ?
                        java.util.Optional.of(explodeDefaultName.get()) : java.util.Optional.empty();
        new ExplodePerGroup(field, isIntField, terms.intTerms, terms.stringTerms, defaultName).execute(session);
    }

    // class for collecting only terms in each group.
    static class TermsCollector implements SimpleIterate.ResultCollector {
        private final LongArrayList[] intTerms;
        private final List<String>[] stringTerms;
        private final boolean isIntField;

        TermsCollector(
                final boolean isIntField,
                final int numGroups) {
            this.isIntField = isIntField;
            if (isIntField) {
                intTerms = new LongArrayList[numGroups + 1];
                stringTerms = null;
            } else {
                stringTerms = new List[numGroups + 1];
                intTerms = null;
            }
        }

        @Override
        public boolean offer(final int group, final TermSelects termSelects) {
            if (isIntField) {
                if (intTerms[group] == null) {
                    intTerms[group] = new LongArrayList(1);
                }
                intTerms[group].add(termSelects.intTerm);
            } else {
                if (stringTerms[group] == null) {
                    stringTerms[group] = new ArrayList<>(1);
                }
                stringTerms[group].add(termSelects.stringTerm);
            }
            return true;
        }

        @Override
        public void finish() {
        }
    }
}

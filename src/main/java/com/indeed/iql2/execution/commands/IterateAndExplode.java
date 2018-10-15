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
import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Commands;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.TermSelects;
import com.indeed.iql2.execution.commands.misc.FieldIterateOpts;
import java.util.function.Consumer;;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
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
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final List<List<List<TermSelects>>> iterationResults = new SimpleIterate(field, fieldOpts, selecting, Collections.nCopies(selecting.size(), Optional.<String>absent()), false, scope).evaluate(session, out);
        final List<Commands.TermsWithExplodeOpts> explodes = Lists.newArrayListWithCapacity(iterationResults.size() + 1);
        explodes.add(null);
        for (final List<List<TermSelects>> groupResults : iterationResults) {
            if (groupResults.size() > 0) {
                final List<TermSelects> groupFieldResults = groupResults.get(0);
                final List<Term> terms = Lists.newArrayListWithCapacity(groupFieldResults.size());
                for (final TermSelects result : groupFieldResults) {
                    terms.add(new Term(result.field, result.isIntTerm, result.intTerm, result.stringTerm));
                }
                explodes.add(new Commands.TermsWithExplodeOpts(terms, this.explodeDefaultName));
            } else {
                explodes.add(new Commands.TermsWithExplodeOpts(Collections.<Term>emptyList(), this.explodeDefaultName));
            }
        }
        new ExplodePerGroup(explodes, field, session.isIntField(field)).execute(session, out);
    }
}

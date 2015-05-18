package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.indeed.common.util.Pair;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
import com.indeed.squall.jql.Commands;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.TermSelects;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class IterateAndExplode {
    public final String field;
    public final List<AggregateMetric> selecting;
    public final Iterate.FieldIterateOpts fieldOpts;
    public final Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits;
    public final Optional<String> explodeDefaultName;

    public IterateAndExplode(String field, List<AggregateMetric> selecting, Iterate.FieldIterateOpts fieldOpts, Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits, Optional<String> explodeDefaultName) {
        this.field = field;
        this.selecting = selecting;
        this.fieldOpts = fieldOpts;
        this.fieldLimits = fieldLimits;
        this.explodeDefaultName = explodeDefaultName;
    }

    public void execute(Session session) throws ImhotepOutOfMemoryException, IOException {
        final List<Iterate.FieldWithOptions> fieldWithOpts = Arrays.asList(new Iterate.FieldWithOptions(this.field, this.fieldOpts));
        final List<List<List<TermSelects>>> iterationResults = new Iterate(fieldWithOpts, this.fieldLimits, this.selecting).execute(session);
        final List<Commands.TermsWithExplodeOpts> explodes = Lists.newArrayList((Commands.TermsWithExplodeOpts) null);
        for (final List<List<TermSelects>> groupResults : iterationResults) {
            final List<TermSelects> groupFieldResults = groupResults.get(0);
            final List<Term> terms = Lists.newArrayListWithCapacity(groupFieldResults.size());
            for (final TermSelects result : groupFieldResults) {
                terms.add(new Term(result.field, result.isIntTerm, result.intTerm, result.stringTerm));
            }
            explodes.add(new Commands.TermsWithExplodeOpts(terms, this.explodeDefaultName));
        }
        new ExplodePerGroup(explodes).execute(session);
    }
}

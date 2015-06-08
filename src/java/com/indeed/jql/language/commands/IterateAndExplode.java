package com.indeed.jql.language.commands;

import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.util.core.Pair;

import java.util.List;

public class IterateAndExplode implements Command {
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

    @Override
    public String toString() {
        return "IterateAndExplode{" +
                "field='" + field + '\'' +
                ", selecting=" + selecting +
                ", fieldOpts=" + fieldOpts +
                ", fieldLimits=" + fieldLimits +
                ", explodeDefaultName=" + explodeDefaultName +
                '}';
    }
}

package com.indeed.squall.iql2.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.execution.ExecutionStep;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public interface GroupBy {
    
    GroupBy transform(
            Function<GroupBy, GroupBy> groupBy, 
            Function<AggregateMetric, AggregateMetric> f, 
            Function<DocMetric, DocMetric> g,
            Function<AggregateFilter, AggregateFilter> h,
            Function<DocFilter, DocFilter> i
    );

    GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f);

    ExecutionStep executionStep(Set<String> scope);

    class GroupByMetric implements GroupBy {
        private final DocMetric metric;
        private final long min;
        private final long max;
        private final long interval;
        private final boolean excludeGutters;
        private final boolean withDefault;

        public GroupByMetric(DocMetric metric, long min, long max, long interval, boolean excludeGutters, boolean withDefault) {
            this.metric = metric;
            this.min = min;
            this.max = max;
            this.interval = interval;
            this.excludeGutters = excludeGutters;
            this.withDefault = withDefault;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByMetric(metric.transform(g, i), min, max, interval, excludeGutters, withDefault));
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeMetric(metric, min, max, interval, scope, excludeGutters, withDefault);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupByMetric that = (GroupByMetric) o;

            if (min != that.min) return false;
            if (max != that.max) return false;
            if (interval != that.interval) return false;
            if (excludeGutters != that.excludeGutters) return false;
            if (withDefault != that.withDefault) return false;
            return !(metric != null ? !metric.equals(that.metric) : that.metric != null);

        }

        @Override
        public int hashCode() {
            int result = metric != null ? metric.hashCode() : 0;
            result = 31 * result + (int) (min ^ (min >>> 32));
            result = 31 * result + (int) (max ^ (max >>> 32));
            result = 31 * result + (int) (interval ^ (interval >>> 32));
            result = 31 * result + (excludeGutters ? 1 : 0);
            result = 31 * result + (withDefault ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GroupByMetric{" +
                    "metric=" + metric +
                    ", min=" + min +
                    ", max=" + max +
                    ", interval=" + interval +
                    ", excludeGutters=" + excludeGutters +
                    ", withDefault=" + withDefault +
                    '}';
        }
    }

    class GroupByTime implements GroupBy {
        private final long periodMillis;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTime(long periodMillis, Optional<String> field, Optional<String> format) {
            this.periodMillis = periodMillis;
            this.field = field;
            this.format = format;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeTimePeriod(periodMillis, field, format);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupByTime that = (GroupByTime) o;

            if (periodMillis != that.periodMillis) return false;
            if (field != null ? !field.equals(that.field) : that.field != null) return false;
            return !(format != null ? !format.equals(that.format) : that.format != null);

        }

        @Override
        public int hashCode() {
            int result = (int) (periodMillis ^ (periodMillis >>> 32));
            result = 31 * result + (field != null ? field.hashCode() : 0);
            result = 31 * result + (format != null ? format.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GroupByTime{" +
                    "periodMillis=" + periodMillis +
                    ", field=" + field +
                    ", format=" + format +
                    '}';
        }
    }

    class GroupByTimeBuckets implements GroupBy {
        private final int numBuckets;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTimeBuckets(int numBuckets, Optional<String> field, Optional<String> format) {
            this.numBuckets = numBuckets;
            this.field = field;
            this.format = format;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeTimeBuckets(numBuckets, field, format);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupByTimeBuckets that = (GroupByTimeBuckets) o;

            if (numBuckets != that.numBuckets) return false;
            if (field != null ? !field.equals(that.field) : that.field != null) return false;
            return !(format != null ? !format.equals(that.format) : that.format != null);

        }

        @Override
        public int hashCode() {
            int result = numBuckets;
            result = 31 * result + (field != null ? field.hashCode() : 0);
            result = 31 * result + (format != null ? format.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GroupByTimeBuckets{" +
                    "numBuckets=" + numBuckets +
                    ", field=" + field +
                    ", format=" + format +
                    '}';
        }
    }

    class GroupByMonth implements GroupBy {
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByMonth(Optional<String> field, Optional<String> format) {
            this.field = field;
            this.format = format;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeMonthOfYear();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupByMonth that = (GroupByMonth) o;

            if (field != null ? !field.equals(that.field) : that.field != null) return false;
            return !(format != null ? !format.equals(that.format) : that.format != null);

        }

        @Override
        public int hashCode() {
            int result = field != null ? field.hashCode() : 0;
            result = 31 * result + (format != null ? format.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GroupByMonth{" +
                    "field=" + field +
                    ", format=" + format +
                    '}';
        }
    }

    class GroupByFieldIn implements GroupBy {
        public final String field;
        public final LongList intTerms;
        public final List<String> stringTerms;
        public final boolean withDefault;

        public GroupByFieldIn(String field, LongList intTerms, List<String> stringTerms, boolean withDefault) {
            this.field = field;
            this.intTerms = intTerms;
            this.stringTerms = stringTerms;
            this.withDefault = withDefault;

            if (Sets.newHashSet(stringTerms).size() != stringTerms.size()) {
                throw new IllegalArgumentException("String terms must be unique: " + stringTerms);
            }
            if (new LongOpenHashSet(intTerms).size() != intTerms.size()) {
                throw new IllegalArgumentException("Int terms must be unique: " + intTerms);
            }
            if (intTerms.size() > 0 && stringTerms.size() > 0) {
                throw new IllegalArgumentException("Cannot have both int terms and string terms.");
            }
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            if (intTerms.size() > 0) {
                return ExecutionStep.ExplodeFieldIn.intExplode(scope, field, intTerms, withDefault);
            } else {
                return ExecutionStep.ExplodeFieldIn.stringExplode(scope, field, stringTerms, withDefault);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupByFieldIn that = (GroupByFieldIn) o;
            return withDefault == that.withDefault &&
                    Objects.equals(field, that.field) &&
                    Objects.equals(intTerms, that.intTerms) &&
                    Objects.equals(stringTerms, that.stringTerms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, intTerms, stringTerms, withDefault);
        }

        @Override
        public String toString() {
            return "GroupByFieldIn{" +
                    "field='" + field + '\'' +
                    ", intTerms=" + intTerms +
                    ", stringTerms=" + stringTerms +
                    ", withDefault=" + withDefault +
                    '}';
        }
    }

    class GroupByField implements GroupBy {
        public final String field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Long> limit;
        public final AggregateMetric metric;
        public final boolean withDefault;
        public final boolean forceNonStreaming;

        public GroupByField(String field, Optional<AggregateFilter> filter, Optional<Long> limit, Optional<AggregateMetric> metric, boolean withDefault, boolean forceNonStreaming) {
            this.field = field;
            this.filter = filter;
            this.limit = limit;
            this.forceNonStreaming = forceNonStreaming;
            this.metric = metric.or(new AggregateMetric.DocStats(new DocMetric.Count()));
            this.withDefault = withDefault;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().transform(f, g, h, i, groupBy));
            } else {
                filter = Optional.absent();
            }
            final AggregateMetric metric = this.metric.transform(f, g, h, i, groupBy);
            return groupBy.apply(new GroupByField(field, filter, limit, Optional.of(metric), withDefault, forceNonStreaming));
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().traverse1(f));
            } else {
                filter = Optional.absent();
            }
            final AggregateMetric metric = f.apply(this.metric);
            return new GroupByField(field, filter, limit, Optional.of(metric), withDefault, forceNonStreaming);
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeAndRegroup(field, filter, limit, metric, withDefault, forceNonStreaming);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupByField that = (GroupByField) o;

            if (withDefault != that.withDefault) return false;
            if (forceNonStreaming != that.forceNonStreaming) return false;
            if (field != null ? !field.equals(that.field) : that.field != null) return false;
            if (filter != null ? !filter.equals(that.filter) : that.filter != null) return false;
            if (limit != null ? !limit.equals(that.limit) : that.limit != null) return false;
            return !(metric != null ? !metric.equals(that.metric) : that.metric != null);

        }

        @Override
        public int hashCode() {
            int result = field != null ? field.hashCode() : 0;
            result = 31 * result + (filter != null ? filter.hashCode() : 0);
            result = 31 * result + (limit != null ? limit.hashCode() : 0);
            result = 31 * result + (metric != null ? metric.hashCode() : 0);
            result = 31 * result + (withDefault ? 1 : 0);
            result = 31 * result + (forceNonStreaming ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GroupByField{" +
                    "field='" + field + '\'' +
                    ", filter=" + filter +
                    ", limit=" + limit +
                    ", metric=" + metric +
                    ", withDefault=" + withDefault +
                    '}';
        }
    }

    class GroupByDayOfWeek implements GroupBy {
        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeDayOfWeek();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof GroupByDayOfWeek;
        }

        @Override
        public int hashCode() {
            return 5843;
        }

        @Override
        public String toString() {
            return "GroupByDayOfWeek{}";
        }
    }

    class GroupBySessionName implements GroupBy {
        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeSessionNames();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof GroupBySessionName;
        }

        @Override
        public int hashCode() {
            return 5903;
        }

        @Override
        public String toString() {
            return "GroupBySessionName{}";
        }
    }

    class GroupByQuantiles implements GroupBy {
        private final String field;
        private final int numBuckets;

        public GroupByQuantiles(String field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodePerDocPercentile(field, numBuckets);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupByQuantiles that = (GroupByQuantiles) o;

            if (numBuckets != that.numBuckets) return false;
            return !(field != null ? !field.equals(that.field) : that.field != null);

        }

        @Override
        public int hashCode() {
            int result = field != null ? field.hashCode() : 0;
            result = 31 * result + numBuckets;
            return result;
        }

        @Override
        public String toString() {
            return "GroupByQuantiles{" +
                    "field='" + field + '\'' +
                    ", numBuckets=" + numBuckets +
                    '}';
        }
    }

    class GroupByPredicate implements GroupBy {
        private final DocFilter docFilter;

        public GroupByPredicate(DocFilter docFilter) {
            this.docFilter = docFilter;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByPredicate(docFilter.transform(g, i)));
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            // TODO: Do this better by making ExplodeMetric take a Map<String, DocMetric>?
            final List<ExecutionStep> steps = new ArrayList<>();
            for (final String dataset : scope) {
                steps.add(new ExecutionStep.ExplodeMetric(docFilter.asZeroOneMetric(dataset), 0, 2, 1, Collections.singleton(dataset), true, false));
            }
            return new ExecutionStep.ExecuteMany(steps);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupByPredicate that = (GroupByPredicate) o;
            return Objects.equals(docFilter, that.docFilter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docFilter);
        }

        @Override
        public String toString() {
            return "GroupByPredicate{" +
                    "docFilter=" + docFilter +
                    '}';
        }
    }
}

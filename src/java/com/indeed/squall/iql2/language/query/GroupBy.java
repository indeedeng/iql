package com.indeed.squall.iql2.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.execution.ExecutionStep;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public interface GroupBy {
    interface Visitor<T, E extends Throwable> {
        T visit(GroupByMetric groupByMetric) throws E;
        T visit(GroupByTime groupByTime) throws E;
        T visit(GroupByTimeBuckets groupByTimeBuckets) throws E;
        T visit(GroupByMonth groupByMonth) throws E;
        T visit(GroupByFieldIn groupByFieldIn) throws E;
        T visit(GroupByField groupByField) throws E;
        T visit(GroupByDayOfWeek groupByDayOfWeek) throws E;
        T visit(GroupBySessionName groupBySessionName) throws E;
        T visit(GroupByQuantiles groupByQuantiles) throws E;
        T visit(GroupByPredicate groupByPredicate) throws E;
    }

    <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;
    
    GroupBy transform(
            Function<GroupBy, GroupBy> groupBy, 
            Function<AggregateMetric, AggregateMetric> f, 
            Function<DocMetric, DocMetric> g,
            Function<AggregateFilter, AggregateFilter> h,
            Function<DocFilter, DocFilter> i
    );

    GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f);

    ExecutionStep executionStep(Set<String> scope);

    boolean isTotal();
    GroupBy makeTotal() throws CannotMakeTotalException;

    class GroupByMetric implements GroupBy {
        public final DocMetric metric;
        public final long min;
        public final long max;
        public final long interval;
        public final boolean excludeGutters;
        public final boolean withDefault;

        public GroupByMetric(DocMetric metric, long min, long max, long interval, boolean excludeGutters, boolean withDefault) {
            this.metric = metric;
            this.min = min;
            this.max = max;
            this.interval = interval;
            this.excludeGutters = excludeGutters;
            this.withDefault = withDefault;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
            final Map<String, DocMetric> perDatasetMetric = Maps.newHashMapWithExpectedSize(scope.size());
            for (final String dataset : scope) {
                perDatasetMetric.put(dataset, metric);
            }
            return new ExecutionStep.ExplodeMetric(perDatasetMetric, min, max, interval, scope, excludeGutters, withDefault);
        }

        @Override
        public boolean isTotal() {
            return withDefault || !excludeGutters;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            if (isTotal()) {
                return this;
            }
            return new GroupByMetric(metric, min, max, interval, true, true);
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
        public final long periodMillis;
        public final Optional<String> field;
        public final Optional<String> format;

        public GroupByTime(long periodMillis, Optional<String> field, Optional<String> format) {
            this.periodMillis = periodMillis;
            this.field = field;
            this.format = format;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isTotal() {
            return true;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return this;
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
        public final int numBuckets;
        public final Optional<String> field;
        public final Optional<String> format;

        public GroupByTimeBuckets(int numBuckets, Optional<String> field, Optional<String> format) {
            this.numBuckets = numBuckets;
            this.field = field;
            this.format = format;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isTotal() {
            return true;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return this;
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
        public final Optional<String> field;
        public final Optional<String> format;

        public GroupByMonth(Optional<String> field, Optional<String> format) {
            this.field = field;
            this.format = format;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isTotal() {
            return true;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return this;
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
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isTotal() {
            return withDefault;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return new GroupByFieldIn(field, intTerms, stringTerms, true);
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
        public final Optional<AggregateMetric> metric;
        public final boolean withDefault;
        public final boolean forceNonStreaming;

        public GroupByField(String field, Optional<AggregateFilter> filter, Optional<Long> limit, Optional<AggregateMetric> metric, boolean withDefault, boolean forceNonStreaming) {
            this.field = field;
            this.filter = filter;
            this.limit = limit;
            this.forceNonStreaming = forceNonStreaming;
            this.metric = limit.isPresent() ? metric.or(Optional.of(new AggregateMetric.DocStats(new DocMetric.Count()))) : metric;
            this.withDefault = withDefault;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().transform(f, g, h, i, groupBy));
            } else {
                filter = Optional.absent();
            }
            final Optional<AggregateMetric> metric;
            if (this.metric.isPresent()) {
                metric = Optional.of(this.metric.get().transform(f, g, h, i, groupBy));
            } else {
                metric = Optional.absent();
            }
            return groupBy.apply(new GroupByField(field, filter, limit, metric, withDefault, forceNonStreaming));
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().traverse1(f));
            } else {
                filter = Optional.absent();
            }
            final Optional<AggregateMetric> metric;
            if (this.metric.isPresent()) {
                metric = Optional.of(f.apply(this.metric.get()));
            } else {
                metric = Optional.absent();
            }
            return new GroupByField(field, filter, limit, metric, withDefault, forceNonStreaming);
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeAndRegroup(field, filter, limit, metric, withDefault, forceNonStreaming);
        }

        @Override
        public boolean isTotal() {
            return withDefault || (!filter.isPresent() && !limit.isPresent());
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return new GroupByField(field, filter, limit, metric, true, forceNonStreaming);
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
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
            return new ExecutionStep.ExplodeDayOfWeek();
        }

        @Override
        public boolean isTotal() {
            return true;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return this;
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
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
            return new ExecutionStep.ExplodeSessionNames();
        }

        @Override
        public boolean isTotal() {
            return true;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return this;
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
        public final String field;
        public final int numBuckets;

        public GroupByQuantiles(String field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isTotal() {
            // TODO: This should be dependent on whether or not every document has said field
            return true;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return this;
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
        public final DocFilter docFilter;

        public GroupByPredicate(DocFilter docFilter) {
            this.docFilter = docFilter;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
            final Map<String, DocMetric> perDatasetMetric = Maps.newHashMapWithExpectedSize(scope.size());
            for (final String dataset : scope) {
                perDatasetMetric.put(dataset, docFilter.asZeroOneMetric(dataset));
            }
            return new ExecutionStep.ExplodeMetric(perDatasetMetric, 0, 2, 1, scope, true, false);
        }

        @Override
        public boolean isTotal() {
            return true;
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return this;
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

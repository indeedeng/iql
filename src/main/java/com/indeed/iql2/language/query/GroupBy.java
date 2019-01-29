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

package com.indeed.iql2.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.AbstractPositional;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.Positional;
import com.indeed.iql2.language.TimePeriods;
import com.indeed.iql2.language.execution.ExecutionStep;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class GroupBy extends AbstractPositional {
    public interface Visitor<T, E extends Throwable> {
        T visit(GroupByMetric groupByMetric) throws E;
        T visit(GroupByTime groupByTime) throws E;
        T visit(GroupByTimeBuckets groupByTimeBuckets) throws E;
        T visit(GroupByInferredTime groupByInferredTime) throws E;
        T visit(GroupByMonth groupByMonth) throws E;
        T visit(GroupByFieldIn groupByFieldIn) throws E;
        T visit(GroupByFieldInQuery groupByFieldInQuery) throws E;
        T visit(GroupByField groupByField) throws E;
        T visit(GroupByDayOfWeek groupByDayOfWeek) throws E;
        T visit(GroupBySessionName groupBySessionName) throws E;
        T visit(GroupByQuantiles groupByQuantiles) throws E;
        T visit(GroupByPredicate groupByPredicate) throws E;
        T visit(GroupByRandom groupByRandom) throws E;
        T visit(GroupByRandomMetric groupByRandom) throws E;
    }

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;
    
    public abstract GroupBy transform(
            Function<GroupBy, GroupBy> groupBy,
            Function<AggregateMetric, AggregateMetric> f,
            Function<DocMetric, DocMetric> g,
            Function<AggregateFilter, AggregateFilter> h,
            Function<DocFilter, DocFilter> i
    );

    public abstract GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f);

    public abstract ExecutionStep executionStep(List<Dataset> datasets);

    public abstract boolean isTotal();
    public abstract GroupBy makeTotal() throws CannotMakeTotalException;

    @Override
    public GroupBy copyPosition(final Positional positional) {
        super.copyPosition(positional);
        return this;
    }

    public static class GroupByMetric extends GroupBy {
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
            return groupBy.apply(new GroupByMetric(metric.transform(g, i), min, max, interval, excludeGutters, withDefault))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            final Map<String, DocMetric> perDatasetMetric = Maps.newHashMapWithExpectedSize(datasets.size());
            final Set<String> scope = Dataset.datasetToScope(datasets);
            for (final String dataset : scope) {
                perDatasetMetric.put(dataset, metric);
            }
            return new ExecutionStep.ExplodeMetric(perDatasetMetric, min, max, interval, scope, excludeGutters, withDefault, false);
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

    public static class GroupByTime extends GroupBy {
        public final long periodMillis;
        public final Optional<FieldSet> field;
        public final Optional<String> format;
        public final boolean isRelative;

        public GroupByTime(long periodMillis, Optional<FieldSet> field, Optional<String> format, boolean isRelative) {
            this.periodMillis = periodMillis;
            this.field = field;
            this.format = format;
            this.isRelative = isRelative;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByTime(periodMillis, field, format, isRelative))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            return new ExecutionStep.ExplodeTimePeriod(periodMillis, field, format, isRelative);
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
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final GroupByTime that = (GroupByTime) o;

            if (periodMillis != that.periodMillis) {
                return false;
            }
            if (isRelative != that.isRelative) {
                return false;
            }
            if (field != null ? !field.equals(that.field) : that.field != null) {
                return false;
            }
            return format != null ? format.equals(that.format) : that.format == null;

        }

        @Override
        public int hashCode() {
            int result = (int) (periodMillis ^ (periodMillis >>> 32));
            result = 31 * result + (field != null ? field.hashCode() : 0);
            result = 31 * result + (format != null ? format.hashCode() : 0);
            result = 31 * result + (isRelative ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GroupByTime{" +
                    "periodMillis=" + periodMillis +
                    ", field=" + field +
                    ", format=" + format +
                    ", isRelative=" + isRelative +
                    '}';
        }
    }

    public static class GroupByInferredTime extends GroupBy {
        public final boolean isRelative;

        public GroupByInferredTime(boolean isRelative) {
            this.isRelative = isRelative;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByInferredTime(isRelative))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            final long periodMillis = TimePeriods.inferTimeBucketSize(Dataset.getEarliestStart(datasets), Dataset.getLatestEnd(datasets));
            return new ExecutionStep.ExplodeTimePeriod(periodMillis, Optional.absent(), Optional.absent(), isRelative);
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
            GroupByInferredTime that = (GroupByInferredTime) o;
            return isRelative == that.isRelative;
        }

        @Override
        public int hashCode() {
            return Objects.hash(isRelative);
        }

        @Override
        public String toString() {
            return "GroupByInferredTime{" +
                    "isRelative=" + isRelative +
                    '}';
        }
    }

    public static class GroupByTimeBuckets extends GroupBy {
        public final int numBuckets;
        public final Optional<FieldSet> field;
        public final Optional<String> format;

        public GroupByTimeBuckets(int numBuckets, Optional<FieldSet> field, Optional<String> format) {
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
            return groupBy.apply(new GroupByTimeBuckets(numBuckets, field, format))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
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

    public static class GroupByMonth extends GroupBy {
        public final Optional<FieldSet> timeField;
        public final Optional<String> timeFormat;

        public GroupByMonth(Optional<FieldSet> field, Optional<String> timeFormat) {
            this.timeField = field;
            this.timeFormat = timeFormat;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByMonth(timeField, timeFormat))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            return new ExecutionStep.ExplodeMonthOfYear(
                    timeField,
                    timeFormat
            );
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

            if (timeField != null ? !timeField.equals(that.timeField) : that.timeField != null) return false;
            return !(timeFormat != null ? !timeFormat.equals(that.timeFormat) : that.timeFormat != null);

        }

        @Override
        public int hashCode() {
            int result = timeField != null ? timeField.hashCode() : 0;
            result = 31 * result + (timeFormat != null ? timeFormat.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GroupByMonth{" +
                    "timeField=" + timeField +
                    ", timeFormat=" + timeFormat +
                    '}';
        }
    }

    public static class GroupByFieldIn extends GroupBy {
        public final FieldSet field;
        public final LongList intTerms;
        public final List<String> stringTerms;
        public final boolean withDefault;

        public GroupByFieldIn(FieldSet field, LongList intTerms, List<String> stringTerms, boolean withDefault) {
            this.field = field;
            this.intTerms = intTerms;
            this.stringTerms = stringTerms;
            this.withDefault = withDefault;

            if (Sets.newHashSet(stringTerms).size() != stringTerms.size()) {
                throw new IqlKnownException.ParseErrorException("String terms must be unique: " + stringTerms);
            }
            if (new LongOpenHashSet(intTerms).size() != intTerms.size()) {
                throw new IqlKnownException.ParseErrorException("Int terms must be unique: " + intTerms);
            }
            if (intTerms.size() > 0 && stringTerms.size() > 0) {
                throw new IqlKnownException.ParseErrorException("Cannot have both int terms and string terms.");
            }
            if (intTerms.size() == 0 && stringTerms.size() == 0) {
                throw new IqlKnownException.ParseErrorException("Cannot have empty field in Set");
            }
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByFieldIn(field, intTerms, stringTerms, withDefault)).copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            if (intTerms.size() > 0) {
                return ExecutionStep.ExplodeFieldIn.intExplode(field, intTerms, withDefault);
            } else {
                return ExecutionStep.ExplodeFieldIn.stringExplode(field, stringTerms, withDefault);
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

    public static class GroupByFieldInQuery extends GroupBy {
        public final FieldSet field;
        public final Query query;
        public final boolean isNegated;
        public final boolean withDefault;
        private final DatasetsMetadata datasetsMetadata;

        public GroupByFieldInQuery(
                final FieldSet field,
                final Query query,
                final boolean isNegated,
                final boolean withDefault,
                final DatasetsMetadata datasetsMetadata) {
            this.field = field;
            this.query = query;
            this.isNegated = isNegated;
            this.withDefault = withDefault;
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(
                final Function<GroupBy, GroupBy> groupBy,
                final Function<AggregateMetric, AggregateMetric> f,
                final Function<DocMetric, DocMetric> g,
                final Function<AggregateFilter, AggregateFilter> h,
                final Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByFieldInQuery(field, query, isNegated, withDefault, datasetsMetadata))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(final List<Dataset> datasets) {
            return new ExecutionStep.GroupByFieldInQueryPlaceholderExecutionStep(field, query, datasetsMetadata);
        }

        @Override
        public boolean isTotal() {
            throw new IllegalStateException("GroupByFieldInQuery must be already transformed into another GroupBy");
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            throw new IllegalStateException("GroupByFieldInQuery must be already transformed into another GroupBy");
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if ((o == null) || (getClass() != o.getClass())) {
                return false;
            }
            final GroupByFieldInQuery that = (GroupByFieldInQuery) o;
            return (isNegated == that.isNegated) &&
                    (withDefault == that.withDefault) &&
                    Objects.equals(field, that.field) &&
                    Objects.equals(query, that.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, query, isNegated, withDefault);
        }

        @Override
        public String toString() {
            return "GroupByFieldIn{" +
                    "field='" + field + '\'' +
                    ", query=" + query +
                    ", isNegated=" + isNegated +
                    ", withDefault=" + withDefault +
                    '}';
        }
    }

    public static class GroupByField extends GroupBy {
        public final FieldSet field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Long> limit;
        public final Optional<AggregateMetric> metric;
        public final boolean withDefault;

        public GroupByField(FieldSet field, Optional<AggregateFilter> filter, Optional<Long> limit, Optional<AggregateMetric> metric, boolean withDefault) {
            this.field = field;
            this.filter = filter;
            this.limit = limit;
            this.metric = limit.isPresent() ? metric.or(Optional.of(new AggregateMetric.DocStats(new DocMetric.Count()))) : metric;
            this.withDefault = withDefault;
        }

        public boolean isTopK() {
            return metric.isPresent() && limit.isPresent();
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
            return groupBy.apply(new GroupByField(field, filter, limit, metric, withDefault))
                    .copyPosition(this);
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
            return new GroupByField(field, filter, limit, metric, withDefault)
                    .copyPosition(this);
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            return new ExecutionStep.ExplodeAndRegroup(field, filter, limit, metric, withDefault);
        }

        @Override
        public boolean isTotal() {
            return withDefault || (!filter.isPresent() && !limit.isPresent());
        }

        @Override
        public GroupBy makeTotal() throws CannotMakeTotalException {
            return new GroupByField(field, filter, limit, metric, true);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupByField that = (GroupByField) o;

            if (withDefault != that.withDefault) return false;
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

    public static class GroupByDayOfWeek extends GroupBy {
        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByDayOfWeek()).copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new GroupByDayOfWeek();
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
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

    public static class GroupBySessionName extends GroupBy {
        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupBySessionName()).copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
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

    public static class GroupByQuantiles extends GroupBy {
        public final FieldSet field;
        public final int numBuckets;

        public GroupByQuantiles(FieldSet field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByQuantiles(field, numBuckets))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
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

    public static class GroupByPredicate extends GroupBy {
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
            return groupBy.apply(new GroupByPredicate(docFilter.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            final Map<String, DocMetric> perDatasetMetric = Maps.newHashMapWithExpectedSize(datasets.size());
            final Set<String> scope = Dataset.datasetToScope(datasets);
            for (final String dataset : scope) {
                perDatasetMetric.put(dataset, docFilter.asZeroOneMetric(dataset));
            }
            return new ExecutionStep.ExplodeMetric(perDatasetMetric, 0, 2, 1, scope, true, false, true);
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

    public static class GroupByRandom extends GroupBy {
        public final FieldSet field;
        public final int k;
        public final String salt;

        public GroupByRandom(FieldSet field, int k, String salt) {
            this.field = field;
            this.k = k;
            this.salt = salt;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByRandom(field, k, salt))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(List<Dataset> datasets) {
            return new ExecutionStep.ExplodeRandom(field, k, salt);
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
            GroupByRandom that = (GroupByRandom) o;
            return k == that.k &&
                    com.google.common.base.Objects.equal(field, that.field) &&
                    com.google.common.base.Objects.equal(salt, that.salt);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(field, k, salt);
        }

        @Override
        public String toString() {
            return "GroupByRandom{" +
                    "field=" + field +
                    ", k=" + k +
                    ", salt='" + salt + '\'' +
                    '}';
        }
    }

    public static class GroupByRandomMetric extends GroupBy {
        public final DocMetric metric;
        public final int k;
        public final String salt;

        public GroupByRandomMetric(final DocMetric metric, final int k, final String salt) {
            this.metric = metric;
            this.k = k;
            this.salt = salt;
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public GroupBy transform(final Function<GroupBy, GroupBy> groupBy,
                                 final Function<AggregateMetric, AggregateMetric> f,
                                 final Function<DocMetric, DocMetric> g,
                                 final Function<AggregateFilter, AggregateFilter> h,
                                 final Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByRandomMetric(metric.transform(g, i), k, salt))
                    .copyPosition(this);
        }

        @Override
        public GroupBy traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(final List<Dataset> datasets) {
            final Map<String, DocMetric> perDatasetMetric = Maps.newHashMapWithExpectedSize(datasets.size());
            final Set<String> scope = Dataset.datasetToScope(datasets);
            for (final String dataset : scope) {
                perDatasetMetric.put(dataset, metric);
            }
            return new ExecutionStep.ExplodeRandomMetric(perDatasetMetric, scope, k, salt);
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
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if ((o == null) || (getClass() != o.getClass())) {
                return false;
            }

            final GroupByRandomMetric that = (GroupByRandomMetric) o;
            return com.google.common.base.Objects.equal(metric, that.metric)
                    && (k == that.k)
                    && com.google.common.base.Objects.equal(salt, that.salt);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(metric, k, salt);
        }

        @Override
        public String toString() {
            return "GroupByRandomMetric{" +
                    "metric =" + metric +
                    ", k=" + k +
                    ", salt='" + salt + '\'' +
                    '}';
        }
    }
}

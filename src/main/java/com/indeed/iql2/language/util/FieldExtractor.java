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

package com.indeed.iql2.language.util;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.ScopedField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jessec
 */

public class FieldExtractor {

	public static class DatasetField {

		@Nullable public String dataset;
		@Nonnull public String field;
		boolean aliasResolved;

		DatasetField(final String field) {
			this.field = field;
		}

		DatasetField(final String field, final String dataset) {
			this.field = field;
			this.dataset = dataset;
		}

		public DatasetField(final String field, final String dataset, final boolean aliasResolved) {
			this.field = field;
			this.dataset = dataset;
			this.aliasResolved = aliasResolved;
		}

		DatasetField(final Positioned<String> positionedField) {
			this.field = positionedField.unwrap();
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			final DatasetField that = (DatasetField) o;
			return aliasResolved == that.aliasResolved &&
					Objects.equal(dataset, that.dataset) &&
					Objects.equal(field, that.field);
		}

		@Override
		public String toString() {
			return "DatasetField{" +
					"dataset='" + dataset + '\'' +
					", field='" + field + '\'' +
					", aliasResolved=" + aliasResolved +
					'}';
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(dataset, field, aliasResolved);
		}

		static Set<DatasetField> fromScopedField(final ScopedField scopedField){
			if (scopedField.scope.size() > 0) {
				return scopedField.scope.stream().map(singleScope -> new DatasetField(scopedField.field.unwrap(), singleScope)).collect(Collectors.toSet());
			} else {
				return ImmutableSet.of(new DatasetField(scopedField.field));
			}
		}
	}

	private static Set<DatasetField> union(final Set<DatasetField> set1, final Set<DatasetField> set2) {
		return Sets.union(set1, set2).immutableCopy();
	};

	public static Set<DatasetField> getDatasetFields(final Query query) {

		Set<DatasetField> datasetFields = Sets.newHashSet();
		if (query.filter.isPresent()) {
			datasetFields.addAll(FieldExtractor.getDatasetFields(query.filter.get()));
		}
		for (final AggregateMetric aggregateMetric : query.selects) {
			datasetFields.addAll(FieldExtractor.getDatasetFields(aggregateMetric));
		}
		for (final GroupByEntry groupByEntry : query.groupBys) {
			datasetFields.addAll(FieldExtractor.getDatasetFields(groupByEntry.groupBy));
			if (groupByEntry.filter.isPresent()) {
				datasetFields.addAll(FieldExtractor.getDatasetFields(groupByEntry.filter.get()));
			}
		}

		datasetFields = fillMissingDatasets(datasetFields, query.datasets);
		datasetFields = resolveAliases(datasetFields, query.datasets);

		return datasetFields;
	}

	private static Set<DatasetField> fillMissingDatasets(final Set<DatasetField> datasetFields, final List<Dataset> datasets) {
		final Set<DatasetField> newDatasetFields  = Sets.newHashSet();
		for (final DatasetField unfilledDatasetField : datasetFields) {
			if (unfilledDatasetField.aliasResolved || unfilledDatasetField.dataset != null) {
				newDatasetFields.add(unfilledDatasetField);
			} else {
				for (final Dataset dataset : datasets) {
					newDatasetFields.add(new DatasetField(unfilledDatasetField.field, dataset.dataset.unwrap()));
				}
			}
		}
		return newDatasetFields;
	}

	private static Set<DatasetField> resolveAliases(final Set<DatasetField> unresolvedDatasetFields, final List<Dataset> datasets) {

		final Map<String, String> aliasToActualDataset = Maps.newHashMap();
		final Map<String, Map<String, String>> datasetToFieldAliases = Maps.newHashMap();

		for (final Dataset dataset : datasets) {
			if (dataset.alias.isPresent()) {
				aliasToActualDataset.put(dataset.alias.get().unwrap(), dataset.dataset.unwrap());
			}
			final Map<String, String> fieldAliases = Maps.newHashMap();
			dataset.fieldAliases.forEach(
					(field, alias) -> {
						fieldAliases.put(field.unwrap(), alias.unwrap());
					}
			);
			datasetToFieldAliases.put(dataset.dataset.unwrap(), fieldAliases);
		}

		final Set<DatasetField> resolvedDatasetFields  = Sets.newHashSet();

		for (final DatasetField unresolvedDatasetField : unresolvedDatasetFields) {

			if (!unresolvedDatasetField.aliasResolved) {
				if (aliasToActualDataset.containsKey(unresolvedDatasetField.dataset)) {
					unresolvedDatasetField.dataset = aliasToActualDataset.get(unresolvedDatasetField.dataset);
				}

				if (datasetToFieldAliases.containsKey(unresolvedDatasetField.dataset)) {
					final Map<String, String> fieldAliases = datasetToFieldAliases.get(unresolvedDatasetField.dataset);
					if (fieldAliases.containsKey(unresolvedDatasetField.field)) {
						unresolvedDatasetField.field = fieldAliases.get(unresolvedDatasetField.field);
					}
				}

				unresolvedDatasetField.aliasResolved = true;
			}
			resolvedDatasetFields.add(unresolvedDatasetField);

		}

		return resolvedDatasetFields;
	}

	@Nonnull
	public static Set<DatasetField> getDatasetFields(final DocFilter docFilter) {

		return docFilter.visit(new DocFilter.Visitor<Set<DatasetField>, RuntimeException>() {

			private <T extends DocFilter.MetricBinop> Set<DatasetField> getFieldsForBinop(final T binop) {
				return union(getDatasetFields(binop.m1), getDatasetFields(binop.m2));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldIs fieldIs) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldIs.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldIsnt fieldIsnt) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldIsnt.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricEqual metricEqual) throws RuntimeException {
				return getFieldsForBinop(metricEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldInQuery fieldInQuery) throws RuntimeException {
				return union(DatasetField.fromScopedField(fieldInQuery.field), getDatasetFields(fieldInQuery.query));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Between between) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(between.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricNotEqual metricNotEqual) throws RuntimeException {
				return getFieldsForBinop(metricNotEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricGt metricGt) throws RuntimeException {
				return getFieldsForBinop(metricGt);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricGte metricGte) throws RuntimeException {
				return getFieldsForBinop(metricGte);

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricLt metricLt) throws RuntimeException {
				return getFieldsForBinop(metricLt);

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricLte metricLte) throws RuntimeException {
				return getFieldsForBinop(metricLte);

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.And and) throws RuntimeException {
				return union(getDatasetFields(and.f1), getDatasetFields(and.f2));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Or or) throws RuntimeException {
				return union(getDatasetFields(or.f1), getDatasetFields(or.f2));

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Ors ors) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				for (final DocFilter filter : ors.filters) {
					set.addAll(getDatasetFields(filter));
				}
				return set;
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Not not) throws RuntimeException {
				return getDatasetFields(not.filter);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Regex regex) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(regex.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.NotRegex notRegex) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(notRegex.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Qualified qualified) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				getDatasetFields(qualified.filter).forEach(
						datasetField -> {
							if (datasetField.dataset == null) {
								for (final String scope : qualified.scope) {
									set.add(new DatasetField(datasetField.field, scope));
								}
							} else {
								set.add(datasetField);
							}
						}
				);
				return set;

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Lucene lucene) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Sample sample) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(sample.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.SampleDocMetric sample) throws RuntimeException {
				return getDatasetFields(sample.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Always always) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Never never) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.StringFieldIn stringFieldIn) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(stringFieldIn.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.IntFieldIn intFieldIn) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(intFieldIn.field));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.ExplainFieldIn explainFieldIn) throws RuntimeException {
				return getDatasetFields(explainFieldIn.query);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldEqual equal) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(equal.field1), new DatasetField(equal.field2));
			}
		});
	}

	@Nonnull
	public static Set<DatasetField> getDatasetFields(final DocMetric docMetric) {

		return docMetric.visit(new DocMetric.Visitor<Set<DatasetField>, RuntimeException>() {

			private <T extends DocMetric.Binop> Set<DatasetField> getFieldsForBinop(final T binop) {
				return union(getDatasetFields(binop.m1), getDatasetFields(binop.m2));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Log log) throws RuntimeException {
				return getDatasetFields(log.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.PushableDocMetric pushableDocMetric) throws RuntimeException {
				return getDatasetFields(pushableDocMetric.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.PerDatasetDocMetric perDatasetDocMetric) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
 				for (final DocMetric metric : perDatasetDocMetric.datasetToMetric.values()) {
					set.addAll(getDatasetFields(metric));
				}//TODO: Is this dataset specific?
				return set;
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Count count) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.DocId count) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Field field) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(field.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Exponentiate exponentiate) throws RuntimeException {
				return getDatasetFields(exponentiate.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Negate negate) throws RuntimeException {
				return getDatasetFields(negate.m1);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Abs abs) throws RuntimeException {
				return getDatasetFields(abs.m1);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Signum signum) throws RuntimeException {
				return getDatasetFields(signum.m1);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Add add) throws RuntimeException {
				return getFieldsForBinop(add);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Subtract subtract) throws RuntimeException {
				return getFieldsForBinop(subtract);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Multiply multiply) throws RuntimeException {
				return getFieldsForBinop(multiply);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Divide divide) throws RuntimeException {
				return getFieldsForBinop(divide);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Modulus modulus) throws RuntimeException {
				return getFieldsForBinop(modulus);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Min min) throws RuntimeException {
				return getFieldsForBinop(min);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Max max) throws RuntimeException {
				return getFieldsForBinop(max);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricEqual metricEqual) throws RuntimeException {
				return getFieldsForBinop(metricEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricNotEqual metricNotEqual) throws RuntimeException {
				return getFieldsForBinop(metricNotEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricLt metricLt) throws RuntimeException {
				return getFieldsForBinop(metricLt);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricLte metricLte) throws RuntimeException {
				return getFieldsForBinop(metricLte);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricGt metricGt) throws RuntimeException {
				return getFieldsForBinop(metricGt);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricGte metricGte) throws RuntimeException {
				return getFieldsForBinop(metricGte);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.RegexMetric regexMetric) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(regexMetric.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.FloatScale floatScale) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(floatScale.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Constant constant) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasIntField hasIntField) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasIntField.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasStringField hasStringField) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasStringField.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.IntTermCount intTermCount) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(intTermCount.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.StrTermCount stringTermCount) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(stringTermCount.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasInt hasInt) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasInt.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasString hasString) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasString.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.IfThenElse ifThenElse) throws RuntimeException {
				return union(getDatasetFields(ifThenElse.trueCase), union(getDatasetFields(ifThenElse.condition), getDatasetFields(ifThenElse.falseCase)));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Qualified qualified) throws RuntimeException {
				return getDatasetFields(qualified.metric).stream().map(
						datasetField -> {
							if(datasetField.dataset == null) {
								datasetField.dataset = qualified.dataset;
							}
							return datasetField;
						})
						.collect(Collectors.toSet());
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Extract extract) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(extract.field));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Lucene lucene) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.FieldEqualMetric equalMetric) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(equalMetric.field1), new DatasetField(equalMetric.field2));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.StringLen hasStringField) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasStringField.field));
			}
		});
	};
	
	
	@Nonnull
	public static Set<DatasetField> getDatasetFields(final GroupBy groupBy) {
		
		return groupBy.visit(new GroupBy.Visitor<Set<DatasetField>, RuntimeException>() {
			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByMetric groupByMetric) throws RuntimeException {
				return getDatasetFields(groupByMetric.metric);
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByTime groupByTime) throws RuntimeException {
				if (groupByTime.field.isPresent()) {
					return ImmutableSet.of(new DatasetField(groupByTime.field.get()));
				}
				//"unixtime" might be implicitly used here.
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByTimeBuckets groupByTimeBuckets) throws RuntimeException {
				if (groupByTimeBuckets.field.isPresent()) {
					return ImmutableSet.of(new DatasetField(groupByTimeBuckets.field.get()));
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByMonth groupByMonth) throws RuntimeException {
				if (groupByMonth.field.isPresent()) {
					return ImmutableSet.of(new DatasetField(groupByMonth.field.get()));
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByFieldIn groupByFieldIn) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(groupByFieldIn.field));
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByField groupByField) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				set.add(new DatasetField(groupByField.field));
				if (groupByField.filter.isPresent()) {
					set.addAll(getDatasetFields(groupByField.filter.get()));
				}
				if (groupByField.metric.isPresent()) {
					set.addAll(getDatasetFields(groupByField.metric.get()));
				}
				return set;
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByDayOfWeek groupByDayOfWeek) throws RuntimeException {
				//"unixtime" is implicitly used here.
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupBySessionName groupBySessionName) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByQuantiles groupByQuantiles) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(groupByQuantiles.field));
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByPredicate groupByPredicate) throws RuntimeException {
				return getDatasetFields(groupByPredicate.docFilter);
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByRandom groupByRandom) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(groupByRandom.field));
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByRandomMetric groupByRandom) throws RuntimeException {
				return getDatasetFields(groupByRandom.metric);
			}
		});
	}
	
	@Nonnull
	public static Set<DatasetField> getDatasetFields(final AggregateMetric aggregateMetric) {

		return aggregateMetric.visit(new AggregateMetric.Visitor<Set<DatasetField>, RuntimeException>() {

			private <T extends AggregateMetric.Binop> Set<DatasetField> getFieldsForBinop(final T binop) {
				return union(getDatasetFields(binop.m1), getDatasetFields(binop.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Add add) throws RuntimeException {
				return getFieldsForBinop(add);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Log log) throws RuntimeException {
				return getDatasetFields(log.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Negate negate) throws RuntimeException {
				return getDatasetFields(negate.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Abs abs) throws RuntimeException {
				return getDatasetFields(abs.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Subtract subtract) throws RuntimeException {
				return getFieldsForBinop(subtract);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Multiply multiply) throws RuntimeException {
				return getFieldsForBinop(multiply);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Divide divide) throws RuntimeException {
				return getFieldsForBinop(divide);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Modulus modulus) throws RuntimeException {
				return getFieldsForBinop(modulus);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Power power) throws RuntimeException {
				return getFieldsForBinop(power);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Parent parent) throws RuntimeException {
				return getDatasetFields(parent.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Lag lag) throws RuntimeException {
				return getDatasetFields(lag.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.IterateLag iterateLag) throws RuntimeException {
				return getDatasetFields(iterateLag.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Window window) throws RuntimeException {
				return getDatasetFields(window.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Qualified qualified) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				getDatasetFields(qualified.metric).forEach(
						datasetField -> {
							if (datasetField.dataset == null) {
								for (final String scope : qualified.scope) {
									set.add(new DatasetField(datasetField.field, scope));
								}
							} else {
								set.add(datasetField);
							}
						}
				);
				return set;
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.DocStatsPushes docStatsPushes) throws RuntimeException {
				return getDatasetFields(docStatsPushes.pushes);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.DocStats docStats) throws RuntimeException {
				return getDatasetFields(docStats.docMetric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Constant constant) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Percentile percentile) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(percentile.field));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Running running) throws RuntimeException {
				return getDatasetFields(running.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Distinct distinct) throws RuntimeException {
				return union(
						ImmutableSet.of(new DatasetField(distinct.field)),
						distinct.filter.isPresent() ? getDatasetFields(distinct.filter.get()) : ImmutableSet.of()
				);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Named named) throws RuntimeException {
				return getDatasetFields(named.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.GroupStatsLookup groupStatsLookup) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.GroupStatsMultiLookup groupStatsMultiLookup) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.SumAcross sumAcross) throws RuntimeException {
				return union(getDatasetFields(sumAcross.groupBy), getDatasetFields(sumAcross.metric));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.IfThenElse ifThenElse) throws RuntimeException {
				return union(getDatasetFields(ifThenElse.condition), union(getDatasetFields(ifThenElse.trueCase), getDatasetFields(ifThenElse.falseCase)));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.FieldMin fieldMin) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldMin.field));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.FieldMax fieldMax) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldMax.field));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Min min) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				for (final AggregateMetric metric : min.metrics) {
					set.addAll(getDatasetFields(metric));
				}
				return set;
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Max max) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				for (final AggregateMetric metric : max.metrics) {
					set.addAll(getDatasetFields(metric));
				}
				return set;
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Bootstrap bootstrap) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				set.add(new DatasetField(bootstrap.field));
				if (bootstrap.filter.isPresent()) {
					set.addAll(getDatasetFields(bootstrap.filter.get()));
				}
				set.addAll(getDatasetFields(bootstrap.metric));
				return set;
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.DivideByCount divideByCount) throws RuntimeException {
				return getDatasetFields(divideByCount.metric);
			}
		});
	} 
	
	@Nonnull
	public static Set<DatasetField> getDatasetFields(final AggregateFilter aggregateFilter) {
		return aggregateFilter.visit(new AggregateFilter.Visitor<Set<DatasetField>, RuntimeException>() {

			@Override
			public Set<DatasetField> visit(final AggregateFilter.TermIs termIs) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.TermRegex termIsRegex) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.MetricIs metricIs) throws RuntimeException {
				return union(getDatasetFields(metricIs.m1), getDatasetFields(metricIs.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.MetricIsnt metricIsnt) throws RuntimeException {
				return union(getDatasetFields(metricIsnt.m1), getDatasetFields(metricIsnt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Gt gt) throws RuntimeException {
				return union(getDatasetFields(gt.m1), getDatasetFields(gt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Gte gte) throws RuntimeException {
				return union(getDatasetFields(gte.m1), getDatasetFields(gte.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Lt lt) throws RuntimeException {
				return union(getDatasetFields(lt.m1), getDatasetFields(lt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Lte lte) throws RuntimeException {
				return union(getDatasetFields(lte.m1), getDatasetFields(lte.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.And and) throws RuntimeException {
				return union(getDatasetFields(and.f1), getDatasetFields(and.f2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Or or) throws RuntimeException {
				return union(getDatasetFields(or.f1), getDatasetFields(or.f2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Not not) throws RuntimeException {
				return getDatasetFields(not.filter);
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Regex regex) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(regex.field));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Always always) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Never never) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.IsDefaultGroup isDefaultGroup) throws RuntimeException {
				return ImmutableSet.of();
			}
		});
	}
}

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
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Query;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jessec
 */

public class FieldExtractor {
	private FieldExtractor() {
	}

	public static class DatasetField {
		@Nonnull public String dataset;
		@Nonnull public final String field;
		boolean aliasResolved;

		DatasetField(final String field, final String dataset) {
			this.field = field;
			this.dataset = dataset;
		}

		public DatasetField(final String field, final String dataset, final boolean aliasResolved) {
			this.field = field;
			this.dataset = dataset;
			this.aliasResolved = aliasResolved;
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
		public int hashCode() {
			return Objects.hashCode(dataset, field, aliasResolved);
		}

		@Override
		public String toString() {
			return "DatasetField{" +
					"dataset='" + dataset + '\'' +
					", field='" + field + '\'' +
					'}';
		}
	}

	private static Set<DatasetField> union(final Set<DatasetField> set1, final Set<DatasetField> set2) {
		return Sets.union(set1, set2).immutableCopy();
	}

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

		datasetFields = resolveAliases(datasetFields, query.datasets);

		return datasetFields;
	}

	private static Set<DatasetField> resolveAliases(final Set<DatasetField> unresolvedDatasetFields, final List<Dataset> datasets) {

		final Map<String, String> aliasToActualDataset = Maps.newHashMap();

		for (final Dataset dataset : datasets) {
			if (dataset.alias.isPresent()) {
				aliasToActualDataset.put(dataset.alias.get().unwrap(), dataset.dataset.unwrap());
			}
		}

		final Set<DatasetField> resolvedDatasetFields  = Sets.newHashSet();

		for (final DatasetField unresolvedDatasetField : unresolvedDatasetFields) {

			if (!unresolvedDatasetField.aliasResolved) {
				if (aliasToActualDataset.containsKey(unresolvedDatasetField.dataset)) {
					unresolvedDatasetField.dataset = aliasToActualDataset.get(unresolvedDatasetField.dataset);
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
			public Set<DatasetField> visit(final DocFilter.FieldIs fieldIs) {
				return fieldIs.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldIsnt fieldIsnt) {
				return fieldIsnt.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricEqual metricEqual) {
				return getFieldsForBinop(metricEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldInQuery fieldInQuery) {
				return Sets.union(fieldInQuery.field.datasetFields(), getDatasetFields(fieldInQuery.query));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Between between) {
				return between.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricNotEqual metricNotEqual) {
				return getFieldsForBinop(metricNotEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricGt metricGt) {
				return getFieldsForBinop(metricGt);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricGte metricGte) {
				return getFieldsForBinop(metricGte);

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricLt metricLt) {
				return getFieldsForBinop(metricLt);

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricLte metricLte) {
				return getFieldsForBinop(metricLte);

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.And and) {
				return getDatasetFields(and);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Or or) {
				return getDatasetFields(or);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Not not) {
				return getDatasetFields(not.filter);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Regex regex) {
				return regex.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.NotRegex notRegex) {
				return notRegex.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Qualified qualified) {
				return getDatasetFields(qualified.filter);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Lucene lucene) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Sample sample) {
				return sample.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.SampleDocMetric sample) {
				return getDatasetFields(sample.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Always always) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Never never) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.StringFieldIn stringFieldIn) {
				return stringFieldIn.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.IntFieldIn intFieldIn) {
				return intFieldIn.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.ExplainFieldIn explainFieldIn) {
				return getDatasetFields(explainFieldIn.query);
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldEqual equal) {
				return Sets.union(equal.field1.datasetFields(), equal.field2.datasetFields());
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
			public Set<DatasetField> visit(final DocMetric.Log log) {
				return getDatasetFields(log.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.PerDatasetDocMetric perDatasetDocMetric) {
				final Set<DatasetField> set = Sets.newHashSet();
 				for (final DocMetric metric : perDatasetDocMetric.datasetToMetric.values()) {
					set.addAll(getDatasetFields(metric));
				}//TODO: Is this dataset specific?
				return set;
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Count count) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.DocId count) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Field field) {
				return field.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Exponentiate exponentiate) {
				return getDatasetFields(exponentiate.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Negate negate) {
				return getDatasetFields(negate.m1);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Abs abs) {
				return getDatasetFields(abs.m1);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Signum signum) {
				return getDatasetFields(signum.m1);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Add add) {
				return getDatasetFieldsForMetrics(add);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Subtract subtract) {
				return getFieldsForBinop(subtract);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Multiply multiply) {
				return getFieldsForBinop(multiply);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Divide divide) {
				return getFieldsForBinop(divide);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Modulus modulus) {
				return getFieldsForBinop(modulus);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Min min) {
				return getDatasetFieldsForMetrics(min);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Max max) {
				return getDatasetFieldsForMetrics(max);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricEqual metricEqual) {
				return getFieldsForBinop(metricEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricNotEqual metricNotEqual) {
				return getFieldsForBinop(metricNotEqual);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricLt metricLt) {
				return getFieldsForBinop(metricLt);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricLte metricLte) {
				return getFieldsForBinop(metricLte);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricGt metricGt) {
				return getFieldsForBinop(metricGt);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricGte metricGte) {
				return getFieldsForBinop(metricGte);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.RegexMetric regexMetric) {
				return regexMetric.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.FloatScale floatScale) {
				return floatScale.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Constant constant) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasIntField hasIntField) {
				return hasIntField.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasStringField hasStringField) {
				return hasStringField.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.IntTermCount intTermCount) {
				return intTermCount.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.StrTermCount stringTermCount) {
				return stringTermCount.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasInt hasInt) {
				return hasInt.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasString hasString) {
				return hasString.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.IfThenElse ifThenElse) {
				return union(getDatasetFields(ifThenElse.trueCase), union(getDatasetFields(ifThenElse.condition), getDatasetFields(ifThenElse.falseCase)));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Qualified qualified) {
				return getDatasetFields(qualified.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Extract extract) {
				return extract.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Lucene lucene) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.FieldEqualMetric equalMetric) {
				return Sets.union(equalMetric.field1.datasetFields(), equalMetric.field2.datasetFields());
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.StringLen hasStringField) {
				return hasStringField.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Sample random) {
				return random.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.SampleMetric random) {
				return getDatasetFields(random.metric);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Random random) {
				return random.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.RandomMetric random) {
				return getDatasetFields(random.metric);
			}
		});
	}


	@Nonnull
	public static Set<DatasetField> getDatasetFields(final GroupBy groupBy) {
		
		return groupBy.visit(new GroupBy.Visitor<Set<DatasetField>, RuntimeException>() {
			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByMetric groupByMetric) {
				return getDatasetFields(groupByMetric.metric);
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByTime groupByTime) {
				if (groupByTime.field.isPresent()) {
					return groupByTime.field.get().datasetFields();
				}
				//"unixtime" might be implicitly used here.
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByTimeBuckets groupByTimeBuckets) {
				if (groupByTimeBuckets.field.isPresent()) {
					return groupByTimeBuckets.field.get().datasetFields();
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByMonth groupByMonth) {
				if (groupByMonth.timeField.isPresent()) {
					return groupByMonth.timeField.get().datasetFields();
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByInferredTime groupByInferredTime) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByFieldIn groupByFieldIn) {
				return groupByFieldIn.field.datasetFields();
			}

			@Override
            public Set<DatasetField> visit(final GroupBy.GroupByFieldInQuery groupByFieldInQuery) {
			    return union(
                        groupByFieldInQuery.field.datasetFields(),
                        getDatasetFields(groupByFieldInQuery.query)
                );
            }

            @Override
			public Set<DatasetField> visit(final GroupBy.GroupByField groupByField) {
				final Set<DatasetField> set = Sets.newHashSet();
				set.addAll(groupByField.field.datasetFields());
				if (groupByField.filter.isPresent()) {
					set.addAll(getDatasetFields(groupByField.filter.get()));
				}
				if (groupByField.isMetricPresent()) {
					set.addAll(getDatasetFields(groupByField.topK.get().metric));
				}
				return set;
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByDayOfWeek groupByDayOfWeek) {
				//"unixtime" is implicitly used here.
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupBySessionName groupBySessionName) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByQuantiles groupByQuantiles) {
				return groupByQuantiles.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByPredicate groupByPredicate) {
				return getDatasetFields(groupByPredicate.docFilter);
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByRandom groupByRandom) {
				return groupByRandom.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByRandomMetric groupByRandom) {
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
			public Set<DatasetField> visit(final AggregateMetric.Add add) {
				return getDatasetFieldsForAggregateMetrics(add.metrics);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Log log) {
				return getDatasetFields(log.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Negate negate) {
				return getDatasetFields(negate.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Abs abs) {
				return getDatasetFields(abs.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Floor floor) {
				return getDatasetFields(floor.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Ceil ceil) {
				return getDatasetFields(ceil.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Round round) {
				return getDatasetFields(round.m1);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Subtract subtract) {
				return getFieldsForBinop(subtract);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Multiply multiply) {
				return getFieldsForBinop(multiply);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Divide divide) {
				return getFieldsForBinop(divide);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Modulus modulus) {
				return getFieldsForBinop(modulus);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Power power) {
				return getFieldsForBinop(power);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Parent parent) {
				return getDatasetFields(parent.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Lag lag) {
				return getDatasetFields(lag.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.IterateLag iterateLag) {
				return getDatasetFields(iterateLag.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Window window) {
				return getDatasetFields(window.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Qualified qualified) {
				return getDatasetFields(qualified.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.DocStatsPushes docStatsPushes) {
				return getDatasetFields(docStatsPushes.pushes);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.DocStats docStats) {
				return getDatasetFields(docStats.docMetric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Constant constant) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Percentile percentile) {
                return percentile.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Running running) {
				return getDatasetFields(running.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Distinct distinct) {
				return union(
						distinct.field.datasetFields(),
						distinct.filter.isPresent() ? getDatasetFields(distinct.filter.get()) : ImmutableSet.of()
				);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Named named) {
				return getDatasetFields(named.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.NeedsSubstitution needsSubstitution) {
				return Collections.emptySet();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.GroupStatsLookup groupStatsLookup) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.SumAcross sumAcross) {
				return union(getDatasetFields(sumAcross.groupBy), getDatasetFields(sumAcross.metric));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.IfThenElse ifThenElse) {
				return union(getDatasetFields(ifThenElse.condition), union(getDatasetFields(ifThenElse.trueCase), getDatasetFields(ifThenElse.falseCase)));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.FieldMin fieldMin) {
				return fieldMin.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.FieldMax fieldMax) {
				return fieldMax.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Min min) {
				return getDatasetFieldsForAggregateMetrics(min.metrics);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Max max) {
				return getDatasetFieldsForAggregateMetrics(max.metrics);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.DivideByCount divideByCount) {
				return getDatasetFields(divideByCount.metric);
			}
		});
	}

	@Nonnull
	public static Set<DatasetField> getDatasetFields(final AggregateFilter aggregateFilter) {
		return aggregateFilter.visit(new AggregateFilter.Visitor<Set<DatasetField>, RuntimeException>() {

			@Override
			public Set<DatasetField> visit(final AggregateFilter.TermIs termIs) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.TermRegex termIsRegex) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.MetricIs metricIs) {
				return union(getDatasetFields(metricIs.m1), getDatasetFields(metricIs.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.MetricIsnt metricIsnt) {
				return union(getDatasetFields(metricIsnt.m1), getDatasetFields(metricIsnt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Gt gt) {
				return union(getDatasetFields(gt.m1), getDatasetFields(gt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Gte gte) {
				return union(getDatasetFields(gte.m1), getDatasetFields(gte.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Lt lt) {
				return union(getDatasetFields(lt.m1), getDatasetFields(lt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Lte lte) {
				return union(getDatasetFields(lte.m1), getDatasetFields(lte.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.And and) {
				return getDatasetFields(and);
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Or or) {
				return getDatasetFields(or);
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Not not) {
				return getDatasetFields(not.filter);
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Regex regex) {
				return regex.field.datasetFields();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Always always) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Never never) {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.IsDefaultGroup isDefaultGroup) {
				return ImmutableSet.of();
			}
		});
	}

	@Nonnull
	private static Set<DatasetField> getDatasetFields(final DocFilter.Multiary multiary) {
		final Set<DatasetField> set = Sets.newHashSet();
		for (final DocFilter filter : multiary.filters) {
			set.addAll(getDatasetFields(filter));
		}
		return set;
	}

	@Nonnull
	private static Set<DatasetField> getDatasetFieldsForMetrics(final DocMetric.Multiary multiary) {
		final Set<DatasetField> set = Sets.newHashSet();
		for (final DocMetric metric : multiary.metrics) {
			set.addAll(getDatasetFields(metric));
		}
		return set;
	}

	@Nonnull
	private static Set<DatasetField> getDatasetFieldsForAggregateMetrics(final Collection<AggregateMetric> metrics) {
		final Set<DatasetField> set = Sets.newHashSet();
		for (final AggregateMetric metric : metrics) {
			set.addAll(getDatasetFields(metric));
		}
		return set;
	}

	@Nonnull
	private static Set<DatasetField> getDatasetFields(final AggregateFilter.Multiary multiary) {
		final Set<DatasetField> set = Sets.newHashSet();
		for (final AggregateFilter filter : multiary.filters) {
			set.addAll(getDatasetFields(filter));
		}
		return set;
	}
}

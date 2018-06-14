package com.indeed.squall.iql2.language.util;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.GroupByEntry;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
	}

	private static Set<DatasetField> mergeSet(final Set<DatasetField> set1, final Set<DatasetField> set2) {
		final Set<DatasetField> mergedSet = Sets.newHashSet(set1);
		mergedSet.addAll(set2);
		return mergedSet;
	};

	public static Set<DatasetField> getDatasetFields(final Query query) {
		final Set<DatasetField> fields = Sets.newHashSet();
		if (query.filter.isPresent()) {
			fields.addAll(FieldExtractor.getDatasetFields(query.filter.get()));
		}
		for (final AggregateMetric aggregateMetric : query.selects) {
			fields.addAll(FieldExtractor.getDatasetFields(aggregateMetric));
		}
		for (final GroupByEntry groupByEntry : query.groupBys) {
			fields.addAll(FieldExtractor.getDatasetFields(groupByEntry.groupBy));
			if (groupByEntry.filter.isPresent()) {
				fields.addAll(FieldExtractor.getDatasetFields(groupByEntry.filter.get()));
			}
		}

		final Map<String, String> aliasToAcutalDataset = Maps.newHashMap();
		for (final Dataset dataset : query.datasets) {
			if (dataset.alias.isPresent()) {
				aliasToAcutalDataset.put(dataset.alias.get().unwrap(), dataset.dataset.unwrap());
			}
		}
		final Map<String, Map<String, String>> datasetToFieldAliases = Maps.newHashMap();
		for (final Dataset dataset : query.datasets) {
			final Map<String, String> fieldAliases = Maps.newHashMap();
			dataset.fieldAliases.forEach(
					(field, alias) -> {
						fieldAliases.put(field.unwrap(), alias.unwrap());
					}
			);
			datasetToFieldAliases.put(dataset.dataset.unwrap(), fieldAliases);
		}

		final Set<DatasetField> datasetFields  = Sets.newHashSet();
		for (final DatasetField unresolvedField : fields) {
			if (unresolvedField.aliasResolved) {
				datasetFields.add(unresolvedField);
				continue;
			}

			if (unresolvedField.dataset != null) {
				datasetFields.add(unresolvedField);
			} else {
				for (final Dataset dataset : query.datasets) {
					datasetFields.add(new DatasetField(unresolvedField.field, dataset.dataset.unwrap()));
				}
			}
		}

		for (final DatasetField unresolvedDatasetField : datasetFields) {

			if (unresolvedDatasetField.aliasResolved) {
				continue;
			}

			if (aliasToAcutalDataset.containsKey(unresolvedDatasetField.dataset)) {
				unresolvedDatasetField.dataset = aliasToAcutalDataset.get(unresolvedDatasetField.dataset);
			}

			final Map<String, String> fieldAliases = datasetToFieldAliases.get(unresolvedDatasetField.dataset);
			if (fieldAliases == null) {
				throw new IllegalArgumentException("");
			}
			if (fieldAliases.containsKey(unresolvedDatasetField.field)) {
				unresolvedDatasetField.field = fieldAliases.get(unresolvedDatasetField.field);
			}

			unresolvedDatasetField.aliasResolved = true;
		}

		return datasetFields;
	}

	@Nonnull
	public static Set<DatasetField> getDatasetFields(final DocFilter docFilter) {

		return docFilter.visit(new DocFilter.Visitor<Set<DatasetField>, RuntimeException>() {

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldIs fieldIs) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldIs.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldIsnt fieldIsnt) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldIsnt.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricEqual metricEqual) throws RuntimeException {
				return mergeSet(getDatasetFields(metricEqual.m1), getDatasetFields(metricEqual.m2));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldInQuery fieldInQuery) throws RuntimeException {

				final Set<DatasetField> set = Sets.newHashSet();
				if (fieldInQuery.field.scope.size() == 1) {
					set.add(new DatasetField(fieldInQuery.field.field.unwrap(), fieldInQuery.field.scope.get(0)));
				} else {
					set.add(new DatasetField(fieldInQuery.field.field.unwrap()));
				}
				set.addAll(getDatasetFields(fieldInQuery.query));
				return set;
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Between between) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(between.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricNotEqual metricNotEqual) throws RuntimeException {
				return mergeSet(getDatasetFields(metricNotEqual.m1), getDatasetFields(metricNotEqual.m2));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricGt metricGt) throws RuntimeException {
				return mergeSet(getDatasetFields(metricGt.m1), getDatasetFields(metricGt.m2));

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricGte metricGte) throws RuntimeException {
				return mergeSet(getDatasetFields(metricGte.m1), getDatasetFields(metricGte.m2));

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricLt metricLt) throws RuntimeException {
				return mergeSet(getDatasetFields(metricLt.m1), getDatasetFields(metricLt.m2));

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.MetricLte metricLte) throws RuntimeException {
				return mergeSet(getDatasetFields(metricLte.m1), getDatasetFields(metricLte.m2));

			}

			@Override
			public Set<DatasetField> visit(final DocFilter.And and) throws RuntimeException {
				return mergeSet(getDatasetFields(and.f1), getDatasetFields(and.f2));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Or or) throws RuntimeException {
				return mergeSet(getDatasetFields(or.f1), getDatasetFields(or.f2));

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
				return ImmutableSet.of(new DatasetField(regex.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.NotRegex notRegex) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(notRegex.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Qualified qualified) throws RuntimeException {
				if (qualified.scope.size() == 1) {
					final String scope = qualified.scope.get(0);
					return getDatasetFields(qualified.filter).stream().map(
							datasetField -> {
								if (datasetField.dataset == null) {
									datasetField.dataset = scope;
								}
								return datasetField;
							}
					).collect(Collectors.toSet());
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Lucene lucene) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.Sample sample) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(sample.field.unwrap()));
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
				return ImmutableSet.of(new DatasetField(stringFieldIn.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.IntFieldIn intFieldIn) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(intFieldIn.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.ExplainFieldIn explainFieldIn) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocFilter.FieldEqual equal) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(equal.field1.unwrap()), new DatasetField(equal.field2.unwrap()));
			}
		});
	}

	@Nonnull
	public static Set<DatasetField> getDatasetFields(final DocMetric docMetric) {

		return docMetric.visit(new DocMetric.Visitor<Set<DatasetField>, RuntimeException>() {

			private <T extends DocMetric.Binop> Set<DatasetField> getFieldsForBinop(final T binop) {
				return mergeSet(getDatasetFields(binop.m1), getDatasetFields(binop.m2));
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
				getFieldsForBinop(metricNotEqual);
				return null;
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricLt metricLt) throws RuntimeException {
				return getFieldsForBinop(metricLt);
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.MetricLte metricLte) throws RuntimeException {
				getFieldsForBinop(metricLte);
				return null;
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
				return ImmutableSet.of(new DatasetField(regexMetric.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.FloatScale floatScale) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(floatScale.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Constant constant) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasIntField hasIntField) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasIntField.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasStringField hasStringField) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasStringField.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.IntTermCount intTermCount) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(intTermCount.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.StrTermCount stringTermCount) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(stringTermCount.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasInt hasInt) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasInt.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.HasString hasString) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasString.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.IfThenElse ifThenElse) throws RuntimeException {
				return mergeSet(getDatasetFields(ifThenElse.trueCase), mergeSet(getDatasetFields(ifThenElse.condition), getDatasetFields(ifThenElse.falseCase)));
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
				return ImmutableSet.of(new DatasetField(extract.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.Lucene lucene) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.FieldEqualMetric equalMetric) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(equalMetric.field1.unwrap()), new DatasetField(equalMetric.field2.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final DocMetric.StringLen hasStringField) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(hasStringField.field.unwrap()));
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
					return ImmutableSet.of(new DatasetField(groupByTime.field.get().unwrap()));
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByTimeBuckets groupByTimeBuckets) throws RuntimeException {
				if (groupByTimeBuckets.field.isPresent()) {
					return ImmutableSet.of(new DatasetField(groupByTimeBuckets.field.get().unwrap()));
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByMonth groupByMonth) throws RuntimeException {
				if (groupByMonth.field.isPresent()) {
					return ImmutableSet.of(new DatasetField(groupByMonth.field.get().unwrap()));
				}
				return null;
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByFieldIn groupByFieldIn) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(groupByFieldIn.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByField groupByField) throws RuntimeException {
				final Set<DatasetField> set = Sets.newHashSet();
				set.add(new DatasetField(groupByField.field.unwrap()));
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
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupBySessionName groupBySessionName) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByQuantiles groupByQuantiles) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(groupByQuantiles.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByPredicate groupByPredicate) throws RuntimeException {
				return getDatasetFields(groupByPredicate.docFilter);
			}

			@Override
			public Set<DatasetField> visit(final GroupBy.GroupByRandom groupByRandom) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(groupByRandom.field.unwrap()));
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
				return mergeSet(getDatasetFields(binop.m1), getDatasetFields(binop.m2));
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
				if (qualified.scope.size() == 1) {
					return getDatasetFields(qualified.metric).stream().map(
							datasetField -> {
								if (datasetField.dataset == null) {
									datasetField.dataset = qualified.scope.get(0);
								}
								return datasetField;
							}
					).collect(Collectors.toSet());
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.DocStatsPushes docStatsPushes) throws RuntimeException {
				//TODO: Is this dataset specific?
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
				return ImmutableSet.of(new DatasetField(percentile.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Running running) throws RuntimeException {
				return getDatasetFields(running.metric);
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.Distinct distinct) throws RuntimeException {
				return mergeSet(
						ImmutableSet.of(new DatasetField(distinct.field.unwrap())),
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
				return mergeSet(getDatasetFields(sumAcross.groupBy), getDatasetFields(sumAcross.metric));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.IfThenElse ifThenElse) throws RuntimeException {
				return mergeSet(getDatasetFields(ifThenElse.condition), mergeSet(getDatasetFields(ifThenElse.trueCase), getDatasetFields(ifThenElse.falseCase)));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.FieldMin fieldMin) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldMin.field.unwrap()));
			}

			@Override
			public Set<DatasetField> visit(final AggregateMetric.FieldMax fieldMax) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(fieldMax.field.unwrap()));
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
				set.add(new DatasetField(bootstrap.field.unwrap()));
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
				return mergeSet(getDatasetFields(metricIs.m1), getDatasetFields(metricIs.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.MetricIsnt metricIsnt) throws RuntimeException {
				return mergeSet(getDatasetFields(metricIsnt.m1), getDatasetFields(metricIsnt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Gt gt) throws RuntimeException {
				return mergeSet(getDatasetFields(gt.m1), getDatasetFields(gt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Gte gte) throws RuntimeException {
				return mergeSet(getDatasetFields(gte.m1), getDatasetFields(gte.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Lt lt) throws RuntimeException {
				return mergeSet(getDatasetFields(lt.m1), getDatasetFields(lt.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Lte lte) throws RuntimeException {
				return mergeSet(getDatasetFields(lte.m1), getDatasetFields(lte.m2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.And and) throws RuntimeException {
				return mergeSet(getDatasetFields(and.f1), getDatasetFields(and.f2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Or or) throws RuntimeException {
				return mergeSet(getDatasetFields(or.f1), getDatasetFields(or.f2));
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Not not) throws RuntimeException {
				return getDatasetFields(not.filter);
			}

			@Override
			public Set<DatasetField> visit(final AggregateFilter.Regex regex) throws RuntimeException {
				return ImmutableSet.of(new DatasetField(regex.field.unwrap()));
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

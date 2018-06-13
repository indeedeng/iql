package com.indeed.squall.iql2.language.util;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.query.GroupBy;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jessec
 */

public class FieldExtracter {

	private static Set<String> mergeSet(final Set<String> set1, final Set<String> set2) {
		final Set<String> mergedSet = Sets.newHashSet(set1);
		mergedSet.addAll(set2);
		return mergedSet;
	};

	@Nonnull
	public static Set<String> getFields(final DocFilter docFilter) {

		return docFilter.visit(new DocFilter.Visitor<Set<String>, RuntimeException>() {

			@Override
			public Set<String> visit(final DocFilter.FieldIs fieldIs) throws RuntimeException {
				return ImmutableSet.of(fieldIs.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.FieldIsnt fieldIsnt) throws RuntimeException {
				return ImmutableSet.of(fieldIsnt.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.MetricEqual metricEqual) throws RuntimeException {
				return mergeSet(getFields(metricEqual.m1), getFields(metricEqual.m2));
			}

			@Override
			public Set<String> visit(final DocFilter.FieldInQuery fieldInQuery) throws RuntimeException {

				if (fieldInQuery.field.scope.size() == 1) {
					return ImmutableSet.of(fieldInQuery.field.scope.get(0) + "." + fieldInQuery.field.field.unwrap());
				} else {
					return ImmutableSet.of(fieldInQuery.field.field.unwrap());
				}

			}

			@Override
			public Set<String> visit(final DocFilter.Between between) throws RuntimeException {
				return ImmutableSet.of(between.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.MetricNotEqual metricNotEqual) throws RuntimeException {
				return mergeSet(getFields(metricNotEqual.m1), getFields(metricNotEqual.m2));
			}

			@Override
			public Set<String> visit(final DocFilter.MetricGt metricGt) throws RuntimeException {
				return mergeSet(getFields(metricGt.m1), getFields(metricGt.m2));

			}

			@Override
			public Set<String> visit(final DocFilter.MetricGte metricGte) throws RuntimeException {
				return mergeSet(getFields(metricGte.m1), getFields(metricGte.m2));

			}

			@Override
			public Set<String> visit(final DocFilter.MetricLt metricLt) throws RuntimeException {
				return mergeSet(getFields(metricLt.m1), getFields(metricLt.m2));

			}

			@Override
			public Set<String> visit(final DocFilter.MetricLte metricLte) throws RuntimeException {
				return mergeSet(getFields(metricLte.m1), getFields(metricLte.m2));

			}

			@Override
			public Set<String> visit(final DocFilter.And and) throws RuntimeException {
				return mergeSet(getFields(and.f1), getFields(and.f2));
			}

			@Override
			public Set<String> visit(final DocFilter.Or or) throws RuntimeException {
				return mergeSet(getFields(or.f1), getFields(or.f2));

			}

			@Override
			public Set<String> visit(final DocFilter.Ors ors) throws RuntimeException {
				final Set<String> set = Sets.newHashSet();
				for (final DocFilter filter : ors.filters) {
					set.addAll(getFields(filter));
				}
				return set;
			}

			@Override
			public Set<String> visit(final DocFilter.Not not) throws RuntimeException {
				return getFields(not.filter);
			}

			@Override
			public Set<String> visit(final DocFilter.Regex regex) throws RuntimeException {
				return ImmutableSet.of(regex.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.NotRegex notRegex) throws RuntimeException {
				return ImmutableSet.of(notRegex.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.Qualified qualified) throws RuntimeException {
				if (qualified.scope.size() == 1) {
					final String scope = qualified.scope.get(0);
					return getFields(qualified.filter).stream().map(field -> scope + "." + field).collect(Collectors.toSet());
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocFilter.Lucene lucene) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocFilter.Sample sample) throws RuntimeException {
				return ImmutableSet.of(sample.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.SampleDocMetric sample) throws RuntimeException {
				return getFields(sample.metric);
			}

			@Override
			public Set<String> visit(final DocFilter.Always always) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocFilter.Never never) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocFilter.StringFieldIn stringFieldIn) throws RuntimeException {
				return ImmutableSet.of(stringFieldIn.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.IntFieldIn intFieldIn) throws RuntimeException {
				return ImmutableSet.of(intFieldIn.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocFilter.ExplainFieldIn explainFieldIn) throws RuntimeException {
				return null;
			}

			@Override
			public Set<String> visit(final DocFilter.FieldEqual equal) throws RuntimeException {
				return ImmutableSet.of(equal.field1.unwrap(), equal.field2.unwrap());
			}
		});
	}

	@Nonnull
	public static Set<String> getFields(final DocMetric docMetric) {

		return docMetric.visit(new DocMetric.Visitor<Set<String>, RuntimeException>() {

			private <T extends DocMetric.Binop> Set<String> getFieldsForBinop(final T binop) {
				return mergeSet(getFields(binop.m1), getFields(binop.m2));
			}

			@Override
			public Set<String> visit(final DocMetric.Log log) throws RuntimeException {
				return getFields(log.metric);
			}

			@Override
			public Set<String> visit(final DocMetric.PushableDocMetric pushableDocMetric) throws RuntimeException {
				return getFields(pushableDocMetric.metric);
			}

			@Override
			public Set<String> visit(final DocMetric.PerDatasetDocMetric perDatasetDocMetric) throws RuntimeException {
				final Set<String> set = Sets.newHashSet();
 				for (final DocMetric metric : perDatasetDocMetric.datasetToMetric.values()) {
					set.addAll(getFields(metric));
				}//TODO: Is this dataset specific?
				return set;
			}

			@Override
			public Set<String> visit(final DocMetric.Count count) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocMetric.DocId count) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocMetric.Field field) throws RuntimeException {
				return ImmutableSet.of(field.field);
			}

			@Override
			public Set<String> visit(final DocMetric.Exponentiate exponentiate) throws RuntimeException {
				return getFields(exponentiate.metric);
			}

			@Override
			public Set<String> visit(final DocMetric.Negate negate) throws RuntimeException {
				return getFields(negate.m1);
			}

			@Override
			public Set<String> visit(final DocMetric.Abs abs) throws RuntimeException {
				return getFields(abs.m1);
			}

			@Override
			public Set<String> visit(final DocMetric.Signum signum) throws RuntimeException {
				return getFields(signum.m1);
			}

			@Override
			public Set<String> visit(final DocMetric.Add add) throws RuntimeException {
				return getFieldsForBinop(add);
			}

			@Override
			public Set<String> visit(final DocMetric.Subtract subtract) throws RuntimeException {
				return getFieldsForBinop(subtract);
			}

			@Override
			public Set<String> visit(final DocMetric.Multiply multiply) throws RuntimeException {
				return getFieldsForBinop(multiply);
			}

			@Override
			public Set<String> visit(final DocMetric.Divide divide) throws RuntimeException {
				return getFieldsForBinop(divide);
			}

			@Override
			public Set<String> visit(final DocMetric.Modulus modulus) throws RuntimeException {
				return getFieldsForBinop(modulus);
			}

			@Override
			public Set<String> visit(final DocMetric.Min min) throws RuntimeException {
				return getFieldsForBinop(min);
			}

			@Override
			public Set<String> visit(final DocMetric.Max max) throws RuntimeException {
				return getFieldsForBinop(max);
			}

			@Override
			public Set<String> visit(final DocMetric.MetricEqual metricEqual) throws RuntimeException {
				return getFieldsForBinop(metricEqual);
			}

			@Override
			public Set<String> visit(final DocMetric.MetricNotEqual metricNotEqual) throws RuntimeException {
				getFieldsForBinop(metricNotEqual);
				return null;
			}

			@Override
			public Set<String> visit(final DocMetric.MetricLt metricLt) throws RuntimeException {
				return getFieldsForBinop(metricLt);
			}

			@Override
			public Set<String> visit(final DocMetric.MetricLte metricLte) throws RuntimeException {
				getFieldsForBinop(metricLte);
				return null;
			}

			@Override
			public Set<String> visit(final DocMetric.MetricGt metricGt) throws RuntimeException {
				return getFieldsForBinop(metricGt);
			}

			@Override
			public Set<String> visit(final DocMetric.MetricGte metricGte) throws RuntimeException {
				return getFieldsForBinop(metricGte);
			}

			@Override
			public Set<String> visit(final DocMetric.RegexMetric regexMetric) throws RuntimeException {
				return ImmutableSet.of(regexMetric.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.FloatScale floatScale) throws RuntimeException {
				return ImmutableSet.of(floatScale.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.Constant constant) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocMetric.HasIntField hasIntField) throws RuntimeException {
				return ImmutableSet.of(hasIntField.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.HasStringField hasStringField) throws RuntimeException {
				return ImmutableSet.of(hasStringField.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.IntTermCount intTermCount) throws RuntimeException {
				return ImmutableSet.of(intTermCount.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.StrTermCount stringTermCount) throws RuntimeException {
				return ImmutableSet.of(stringTermCount.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.HasInt hasInt) throws RuntimeException {
				return ImmutableSet.of(hasInt.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.HasString hasString) throws RuntimeException {
				return ImmutableSet.of(hasString.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.IfThenElse ifThenElse) throws RuntimeException {
				return mergeSet(getFields(ifThenElse.trueCase), mergeSet(getFields(ifThenElse.condition), getFields(ifThenElse.falseCase)));
			}

			@Override
			public Set<String> visit(final DocMetric.Qualified qualified) throws RuntimeException {
				return getFields(qualified.metric).stream().map(field -> qualified.dataset + "." + field).collect(Collectors.toSet());
			}

			@Override
			public Set<String> visit(final DocMetric.Extract extract) throws RuntimeException {
				return ImmutableSet.of(extract.field.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.Lucene lucene) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final DocMetric.FieldEqualMetric equalMetric) throws RuntimeException {
				return ImmutableSet.of(equalMetric.field1.unwrap(), equalMetric.field2.unwrap());
			}

			@Override
			public Set<String> visit(final DocMetric.StringLen hasStringField) throws RuntimeException {
				return ImmutableSet.of(hasStringField.field.unwrap());
			}
		});
	};
	
	
	@Nonnull
	public static Set<String> getFields(final GroupBy groupBy) {
		
		return groupBy.visit(new GroupBy.Visitor<Set<String>, RuntimeException>() {
			@Override
			public Set<String> visit(final GroupBy.GroupByMetric groupByMetric) throws RuntimeException {
				return getFields(groupByMetric.metric);
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByTime groupByTime) throws RuntimeException {
				if (groupByTime.field.isPresent()) {
					return ImmutableSet.of(groupByTime.field.get().unwrap());
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByTimeBuckets groupByTimeBuckets) throws RuntimeException {
				if (groupByTimeBuckets.field.isPresent()) {
					return ImmutableSet.of(groupByTimeBuckets.field.get().unwrap());
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByMonth groupByMonth) throws RuntimeException {
				if (groupByMonth.field.isPresent()) {
					return ImmutableSet.of(groupByMonth.field.get().unwrap());
				}
				return null;
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByFieldIn groupByFieldIn) throws RuntimeException {
				return ImmutableSet.of(groupByFieldIn.field.unwrap());
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByField groupByField) throws RuntimeException {
				final Set<String> set = Sets.newHashSet();
				set.add(groupByField.field.unwrap());
				if (groupByField.filter.isPresent()) {
					set.addAll(getFields(groupByField.filter.get()));
				}
				if (groupByField.metric.isPresent()) {
					set.addAll(getFields(groupByField.metric.get()));
				}
				return set;
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByDayOfWeek groupByDayOfWeek) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final GroupBy.GroupBySessionName groupBySessionName) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByQuantiles groupByQuantiles) throws RuntimeException {
				return ImmutableSet.of(groupByQuantiles.field.unwrap());
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByPredicate groupByPredicate) throws RuntimeException {
				return getFields(groupByPredicate.docFilter);
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByRandom groupByRandom) throws RuntimeException {
				return ImmutableSet.of(groupByRandom.field.unwrap());
			}

			@Override
			public Set<String> visit(final GroupBy.GroupByRandomMetric groupByRandom) throws RuntimeException {
				return getFields(groupByRandom.metric);
			}
		});
	}
	
	@Nonnull
	public static Set<String> getFields(final AggregateMetric aggregateMetric) {

		return aggregateMetric.visit(new AggregateMetric.Visitor<Set<String>, RuntimeException>() {

			private <T extends AggregateMetric.Binop> Set<String> getFieldsForBinop(final T binop) {
				return mergeSet(getFields(binop.m1), getFields(binop.m2));
			}

			@Override
			public Set<String> visit(final AggregateMetric.Add add) throws RuntimeException {
				return getFieldsForBinop(add);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Log log) throws RuntimeException {
				return getFields(log.m1);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Negate negate) throws RuntimeException {
				return getFields(negate.m1);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Abs abs) throws RuntimeException {
				return getFields(abs.m1);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Subtract subtract) throws RuntimeException {
				return getFieldsForBinop(subtract);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Multiply multiply) throws RuntimeException {
				return getFieldsForBinop(multiply);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Divide divide) throws RuntimeException {
				return getFieldsForBinop(divide);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Modulus modulus) throws RuntimeException {
				return getFieldsForBinop(modulus);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Power power) throws RuntimeException {
				return getFieldsForBinop(power);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Parent parent) throws RuntimeException {
				return getFields(parent);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Lag lag) throws RuntimeException {
				return getFields(lag.metric);
			}

			@Override
			public Set<String> visit(final AggregateMetric.IterateLag iterateLag) throws RuntimeException {
				return getFields(iterateLag.metric);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Window window) throws RuntimeException {
				return getFields(window.metric);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Qualified qualified) throws RuntimeException {
				if (qualified.scope.size() == 1) {
					return getFields(qualified.metric).stream().map(field -> qualified.scope.get(0) + "." + field).collect(Collectors.toSet());
				}
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateMetric.DocStatsPushes docStatsPushes) throws RuntimeException {
				//TODO: Is this dataset specific?
				return getFields(docStatsPushes.pushes);
			}

			@Override
			public Set<String> visit(final AggregateMetric.DocStats docStats) throws RuntimeException {
				return getFields(docStats.docMetric);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Constant constant) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateMetric.Percentile percentile) throws RuntimeException {
				return ImmutableSet.of(percentile.field.unwrap());
			}

			@Override
			public Set<String> visit(final AggregateMetric.Running running) throws RuntimeException {
				return getFields(running.metric);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Distinct distinct) throws RuntimeException {
				return mergeSet(
						ImmutableSet.of(distinct.field.unwrap()),
						distinct.filter.isPresent() ? getFields(distinct.filter.get()) : ImmutableSet.of()
				);
			}

			@Override
			public Set<String> visit(final AggregateMetric.Named named) throws RuntimeException {
				return getFields(named.metric);
			}

			@Override
			public Set<String> visit(final AggregateMetric.GroupStatsLookup groupStatsLookup) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateMetric.GroupStatsMultiLookup groupStatsMultiLookup) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateMetric.SumAcross sumAcross) throws RuntimeException {
				return mergeSet(getFields(sumAcross.groupBy), getFields(sumAcross.metric));
			}

			@Override
			public Set<String> visit(final AggregateMetric.IfThenElse ifThenElse) throws RuntimeException {
				return mergeSet(getFields(ifThenElse.condition), mergeSet(getFields(ifThenElse.trueCase), getFields(ifThenElse.falseCase)));
			}

			@Override
			public Set<String> visit(final AggregateMetric.FieldMin fieldMin) throws RuntimeException {
				return ImmutableSet.of(fieldMin.field.unwrap());
			}

			@Override
			public Set<String> visit(final AggregateMetric.FieldMax fieldMax) throws RuntimeException {
				return ImmutableSet.of(fieldMax.field.unwrap());
			}

			@Override
			public Set<String> visit(final AggregateMetric.Min min) throws RuntimeException {
				final Set<String> set = Sets.newHashSet();
				for (final AggregateMetric metric : min.metrics) {
					set.addAll(getFields(metric));
				}
				return set;
			}

			@Override
			public Set<String> visit(final AggregateMetric.Max max) throws RuntimeException {
				final Set<String> set = Sets.newHashSet();
				for (final AggregateMetric metric : max.metrics) {
					set.addAll(getFields(metric));
				}
				return set;
			}

			@Override
			public Set<String> visit(final AggregateMetric.Bootstrap bootstrap) throws RuntimeException {
				final Set<String> set = Sets.newHashSet();
				set.add(bootstrap.field.unwrap());
				if (bootstrap.filter.isPresent()) {
					set.addAll(getFields(bootstrap.filter.get()));
				}
				set.addAll(getFields(bootstrap.metric));
				return set;
			}

			@Override
			public Set<String> visit(final AggregateMetric.DivideByCount divideByCount) throws RuntimeException {
				return getFields(divideByCount.metric);
			}
		});
	} 
	
	@Nonnull
	public static Set<String> getFields(final AggregateFilter aggregateFilter) {
		return aggregateFilter.visit(new AggregateFilter.Visitor<Set<String>, RuntimeException>() {
			@Override
			public Set<String> visit(final AggregateFilter.TermIs termIs) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateFilter.TermRegex termIsRegex) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateFilter.MetricIs metricIs) throws RuntimeException {
				return mergeSet(getFields(metricIs.m1), getFields(metricIs.m2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.MetricIsnt metricIsnt) throws RuntimeException {
				return mergeSet(getFields(metricIsnt.m1), getFields(metricIsnt.m2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.Gt gt) throws RuntimeException {
				return mergeSet(getFields(gt.m1), getFields(gt.m2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.Gte gte) throws RuntimeException {
				return mergeSet(getFields(gte.m1), getFields(gte.m2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.Lt lt) throws RuntimeException {
				return mergeSet(getFields(lt.m1), getFields(lt.m2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.Lte lte) throws RuntimeException {
				return mergeSet(getFields(lte.m1), getFields(lte.m2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.And and) throws RuntimeException {
				return mergeSet(getFields(and.f1), getFields(and.f2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.Or or) throws RuntimeException {
				return mergeSet(getFields(or.f1), getFields(or.f2));
			}

			@Override
			public Set<String> visit(final AggregateFilter.Not not) throws RuntimeException {
				return getFields(not.filter);
			}

			@Override
			public Set<String> visit(final AggregateFilter.Regex regex) throws RuntimeException {
				return ImmutableSet.of(regex.field.unwrap());
			}

			@Override
			public Set<String> visit(final AggregateFilter.Always always) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateFilter.Never never) throws RuntimeException {
				return ImmutableSet.of();
			}

			@Override
			public Set<String> visit(final AggregateFilter.IsDefaultGroup isDefaultGroup) throws RuntimeException {
				return ImmutableSet.of();
			}
		});
	}
}

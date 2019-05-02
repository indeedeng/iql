package com.indeed.iql2.server.web.servlets.dataset;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class AllData {
    public static final Dataset DATASET;
    static {
        final List<Dataset> datasets = new ArrayList<>();
        datasets.add(DatasetGroupByDataset.createDataset());
        datasets.add(DimensionDataset.createDataset());
        datasets.add(DistinctDataset.createDataset());
        datasets.add(ExtractDataset.createDataset());
        datasets.add(FieldEqualDataset.create());
        datasets.add(FieldInQueryDataset.createAll());
        datasets.add(FloatScaleDataset.createDataset());
        datasets.add(GroupByHavingDataset.createDataset());
        datasets.add(GroupBySelectDataset.createDataset());
        datasets.add(JobsearchDataset.create());
        datasets.add(MultipleDataset.create());
        datasets.add(MultiValueDataset.createDataset());
        datasets.add(MultiValuedDataset.create());
        datasets.add(OrganicDataset.create());
        datasets.add(OrganicDataset.createWithDynamicShardNaming());
        datasets.add(QuantilesDataset.createDataset());
        datasets.add(RegroupEmptyFieldDataset.createDataset());
        datasets.add(StringAsIntFieldDataset.create());
        datasets.add(StringLenDataset.create());
        datasets.add(StringEscapeDataset.create());
        datasets.add(SubQueryLimitDataset.createDataset());
        datasets.add(TimeRegroupDatasets.dayOfWeekDataset());
        datasets.add(TimeRegroupDatasets.multiMonthDataset());
        datasets.add(TSVEscapeDataset.createDataset());
        datasets.add(CSVEscapeDataset.createDataset());
        datasets.add(ValidationDataset.createDataset());
        datasets.add(BigDataset.create());
        datasets.add(ExactCaseDataset.createDataset());
        datasets.add(SyntheticDataset.createDataset());
        datasets.add(CountriesDataset.createDataset());
        datasets.add(SnapshotDataset.create());
        // Defines keywords, from, where, group, select, and limit
        datasets.add(KeywordDatasets.create());
        datasets.add(ConflictFieldDataset.create());
        datasets.add(LogLossDataset.create());
        datasets.add(MultiYearDataset.create());

        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        for (final Dataset allDataset : datasets) {
            shards.addAll(allDataset.shards);
        }
        DATASET = new Dataset(shards);
    }
}

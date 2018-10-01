package com.indeed.iql2.server.web.servlets.dataset;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class AllDatasets {
    static {
        final List<Dataset> allDatasets = new ArrayList<>();

        allDatasets.add(DatasetGroupByDataset.createDataset());
        allDatasets.add(DimensionDataset.createDataset());
        allDatasets.add(DistinctDataset.createDataset());
        allDatasets.add(ExtractDataset.createDataset());
        allDatasets.add(FieldEqualDataset.create());
        allDatasets.addAll(FieldInQueryDataset.createAll());
        allDatasets.add(FloatScaleDataset.createDataset());
        allDatasets.add(GroupByHavingDataset.createDataset());
        allDatasets.add(GroupBySelectDataset.createDataset());
        allDatasets.add(JobsearchDataset.create());
        allDatasets.add(MultipleDataset.create());
        allDatasets.add(MultiValueDataset.createDataset());
        allDatasets.add(MultiValuedDataset.create());
        allDatasets.add(OrganicDataset.create());
        allDatasets.add(QuantilesDataset.createDataset());
        allDatasets.add(RegroupEmptyFieldDataset.createDataset());
        allDatasets.add(StringAsIntFieldDataset.create());
        allDatasets.add(StringLenDataset.create());
        allDatasets.add(SubQueryLimitDataset.createDataset());
        allDatasets.add(TimeRegroupDatasets.dayOfWeekDataset());
        allDatasets.add(TimeRegroupDatasets.multiMonthDataset());
        allDatasets.add(TSVEscapeDataset.createDataset());
        allDatasets.add(ValidationDataset.createDataset());
    }
}

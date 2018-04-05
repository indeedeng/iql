package com.indeed.imhotep.client;

import com.indeed.imhotep.DatasetInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DumbImhotepClientMetadataReloader extends ImhotepClientMetadataReloader {
    private final Map<String, DatasetInfo> datasetToDatasetInfo;

    public DumbImhotepClientMetadataReloader(Map<String, DatasetInfo> datasetToDatasetInfo) {
        super(null, null);
        this.datasetToDatasetInfo = datasetToDatasetInfo;
    }

    @Override
    public boolean load() {
        return true;
    }

    @Override
    public Map<String, DatasetInfo> getDatasetToDatasetInfo() {
        // TODO: deep copy for unmodifiable lists?
        return Collections.unmodifiableMap(datasetToDatasetInfo);
    }
}

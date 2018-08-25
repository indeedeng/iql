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

package com.indeed.imhotep.client;

import com.indeed.imhotep.DatasetInfo;

import java.util.Collections;
import java.util.Map;

public class DumbImhotepClientMetadataReloader extends ImhotepClientMetadataReloader {
    private final Map<String, DatasetInfo> datasetToDatasetInfo;

    public DumbImhotepClientMetadataReloader(Map<String, DatasetInfo> datasetToDatasetInfo) {
        super(null);
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

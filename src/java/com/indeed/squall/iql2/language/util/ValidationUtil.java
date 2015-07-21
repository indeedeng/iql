package com.indeed.squall.iql2.language.util;

import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;

import java.util.Map;
import java.util.Set;

public class ValidationUtil {
    public static DatasetsFields findFieldsUsed(DocMetric docMetric) {
        return DatasetsFields.builder().build();
    }

    public static DatasetsFields findFieldsUsed(DocFilter docFilter) {
        return DatasetsFields.builder().build();
    }
}

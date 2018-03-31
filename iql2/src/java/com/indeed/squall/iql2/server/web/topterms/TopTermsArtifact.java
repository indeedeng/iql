package com.indeed.squall.iql2.server.web.topterms;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author vladimir
 */
public class TopTermsArtifact implements Serializable {
    static final long serialVersionUID = 76L;

    public final long timestamp;
    public final Map<String, Map<String, List<String>>> datasetToFieldToTerms;

    public TopTermsArtifact(long timestamp, Map<String, Map<String, List<String>>> datasetToFieldToTerms) {
        this.timestamp = timestamp;
        this.datasetToFieldToTerms = datasetToFieldToTerms;
    }
}
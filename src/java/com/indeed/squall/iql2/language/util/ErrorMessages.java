package com.indeed.squall.iql2.language.util;

public class ErrorMessages {
    public static String missingStringField(String dataset, String field, Object context) {
        return "Dataset \"" + dataset + "\" does not contain expected string field \"" + field + "\" in [" + context + "]";
    }

    public static String missingIntField(String dataset, String field, Object context) {
        return "Dataset \"" + dataset + "\" does not contain expected int field \"" + field + "\" in [" + context + "]";
    }

    public static String missingField(String dataset, String field, Object context) {
        return "Dataset \"" + dataset + "\" does not contain expected field \"" + field + "\" in [" + context + "]";
    }

    public static String nonAliasMetricInPrecomputation(String field, String expression) {
        return String.format("Non alias metric %s: %s is not allowed in function that require FTGS", field, expression);
    }

    public static String missingDataset(String dataset) {
        return "Expected dataset \"" + dataset + "\" does not exist";
    }

    public static String scopeMismatch(String scope1, String scope2) {
        return String.format("scope mismatches between equality fields [%s != %s]", scope1, scope2);
    }
}

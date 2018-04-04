package com.indeed.squall.iql2.language.util;

public class ErrorMessages {
    public static String missingStringField(String dataset, String field, Object context) {
        return "Dataset \"" + dataset + "\" does not contain expected string field \"" + field + "\" in [" + context + "]";
    }

    public static String missingIntField(String dataset, String field, Object context) {
        return "Dataset \"" + dataset + "\" does not contain expected int field \"" + field + "\" in [" + context + "]";
    }

    public static String stringFieldMismatch(String dataset, String field, Object context) {
        return "Field \"" + field + "\" in Dataset \"" + dataset + "\" is a string field but it is used as an int field in [" + context + "]";
    }

    public static String missingField(String dataset, String field, Object context) {
        return "Dataset \"" + dataset + "\" does not contain expected field \"" + field + "\" in [" + context + "]";
    }

    public static String missingDataset(String dataset) {
        return "Expected dataset \"" + dataset + "\" does not exist";
    }

    public static String nonAliasMetricInFTGS(String field, Object context) {
        return String.format("For functions that requires FTGS, non alias metric \"%s\" is not allowed, [ %s ]", field, context);
    }

    public static String metricFieldIsNotSupported(String field, Object context) {
        return String.format("metric field '%s' is not supported in context: [ %s ]", field, context);
    }

    public static String scopeMismatch(String scope1, String scope2) {
        return String.format("scope mismatches between equality fields [%s != %s]", scope1, scope2);
    }
}

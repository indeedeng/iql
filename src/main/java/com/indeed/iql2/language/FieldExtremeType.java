package com.indeed.iql2.language;

public enum FieldExtremeType {
    FIELDMIN,
    FIELDMAX;

    public static SortOrder toSortOrder(FieldExtremeType fieldExtremeType) {
        return fieldExtremeType == FieldExtremeType.FIELDMAX ? SortOrder.DESCENDING : SortOrder.ASCENDING;
    }
}

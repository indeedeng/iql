package com.indeed.iql2.language;

public enum FieldExtremeType {
    FIELD_MIN(SortOrder.ASCENDING),
    FIELD_MAX(SortOrder.DESCENDING);
    private final SortOrder sortOrder;

    FieldExtremeType(final SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public SortOrder toSortOrder() {
        return sortOrder;
    }
}

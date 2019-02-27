package com.indeed.iql2.language;

public enum SortOrder {
    ASCENDING,
    DESCENDING;

    public static com.indeed.imhotep.protobuf.SortOrder toProtobufSortOrder(SortOrder sortOrder) {
        return sortOrder == SortOrder.ASCENDING ? com.indeed.imhotep.protobuf.SortOrder.ASCENDING: com.indeed.imhotep.protobuf.SortOrder.DESCENDING;
    }
}

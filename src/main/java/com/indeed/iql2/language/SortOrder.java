package com.indeed.iql2.language;

import com.indeed.imhotep.protobuf.StatsSortOrder;

public enum SortOrder {
    ASCENDING(StatsSortOrder.ASCENDING),
    DESCENDING(StatsSortOrder.DESCENDING);
    private StatsSortOrder statsSortOrder;

    SortOrder(final StatsSortOrder statsSortOrder) {
        this.statsSortOrder = statsSortOrder;
    }

    public StatsSortOrder toProtobufSortOrder() {
        return this.statsSortOrder;
    }
}

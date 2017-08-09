package com.indeed.imhotep.web;

import com.indeed.imhotep.exceptions.GroupLimitExceededException;

import javax.annotation.Nullable;
import java.text.DecimalFormat;

public class Limits {
    @Nullable public final Integer queryDocumentCountLimitBillions;
    @Nullable public final Integer queryInMemoryRowsLimit;
    @Nullable public final Integer queryFTGSIQLLimitMB;
    @Nullable public final Integer queryFTGSImhotepDaemonLimitMB;
    @Nullable public final Integer concurrentQueriesLimit;
    @Nullable public final Integer concurrentImhotepSessionsLimit;

    public Limits(Integer queryDocumentCountLimitBillions, Integer queryInMemoryRowsLimit, Integer queryFTGSIQLLimitMB, Integer queryFTGSImhotepDaemonLimitMB, Integer concurrentQueriesLimit, Integer concurrentImhotepSessionsLimit) {
        this.queryDocumentCountLimitBillions = queryDocumentCountLimitBillions;
        this.queryInMemoryRowsLimit = queryInMemoryRowsLimit;
        this.queryFTGSIQLLimitMB = queryFTGSIQLLimitMB;
        this.queryFTGSImhotepDaemonLimitMB = queryFTGSImhotepDaemonLimitMB;
        this.concurrentQueriesLimit = concurrentQueriesLimit;
        this.concurrentImhotepSessionsLimit = concurrentImhotepSessionsLimit;
    }

    public boolean satisfiesQueryDocumentCountLimit(long value) {
        return queryDocumentCountLimitBillions == null || value <= (long)queryDocumentCountLimitBillions * 1_000_000_000;
    }

    public boolean satisfiesQueryInMemoryRowsLimit(double value) {
        return queryInMemoryRowsLimit == null || value <= queryInMemoryRowsLimit;
    }

    public void assertQueryInMemoryRowsLimit(double newNumGroups) {
        if(!satisfiesQueryInMemoryRowsLimit(newNumGroups)) {
            DecimalFormat df = new DecimalFormat("###,###");
            throw new GroupLimitExceededException("Number of groups " + df.format(newNumGroups) + " exceeds the limit " + df.format(queryInMemoryRowsLimit)+
                    ". Please simplify the query.");
        }
    }

    public boolean satisfiesQueryInMemoryRowsLimit(int value) {
        return queryInMemoryRowsLimit == null || value <= queryInMemoryRowsLimit;
    }

    public boolean satisfiesQueryFTGSIQLLimitMB(int value) {
        return queryFTGSIQLLimitMB == null || value <= queryFTGSIQLLimitMB;
    }

    public boolean satisfiesQueryFTGSImhotepDaemonLimitMB(int value) {
        return queryFTGSImhotepDaemonLimitMB == null || value <= queryFTGSImhotepDaemonLimitMB;
    }

    public boolean satisfiesConcurrentQueriesLimit(int value) {
        return concurrentQueriesLimit == null || value <= concurrentQueriesLimit;
    }

    public boolean satisfiesConcurrentImhotepSessionsLimit(int value) {
        return concurrentImhotepSessionsLimit == null || value <= concurrentImhotepSessionsLimit;
    }
}

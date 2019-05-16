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

package com.indeed.iql.web;

import com.indeed.iql.exceptions.IqlKnownException;

import javax.annotation.Nullable;

public class Limits {
    public final byte priority;
    @Nullable public final Integer queryDocumentCountLimitBillions;
    @Nullable public final Integer queryInMemoryRowsLimit;
    @Nullable public final Integer queryFTGSIQLLimitMB;
    @Nullable public final Integer queryFTGSImhotepDaemonLimitMB;
    @Nullable public final Integer concurrentQueriesLimit;
    @Nullable public final Integer concurrentImhotepSessionsLimit;

    public Limits(byte priority, Integer queryDocumentCountLimitBillions, Integer queryInMemoryRowsLimit, Integer queryFTGSIQLLimitMB, Integer queryFTGSImhotepDaemonLimitMB, Integer concurrentQueriesLimit, Integer concurrentImhotepSessionsLimit) {
        this.priority = priority;
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

    public boolean satisfiesQueryInMemoryRowsLimit(final long value) {
        return (queryInMemoryRowsLimit == null) || (value <= queryInMemoryRowsLimit);
    }

    public void assertQueryInMemoryRowsLimit(final long newNumGroups) {
        if(!satisfiesQueryInMemoryRowsLimit(newNumGroups)) {
            throw new IqlKnownException.GroupLimitExceededException("Number of groups [" + newNumGroups + "] exceeds the group limit [" + queryInMemoryRowsLimit + "]"+
                    ". Please simplify the query.");
        }
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

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

package com.indeed.iql2.execution;

/**
 * @author aibragimov
 *
 * This is list of all supported options with description gathered in one place.
 */
public class QueryOptions {

    // Don't do cache lookup, force run query.
    public static final String NO_CACHE = "nocache";

    // Enable port forwarding to use rust daemon
    public static final String USE_RUST_DAEMON = "rust";

    // Fail after executing last command
    public static final String DIE_AT_END = "die_at_end";

    // Temporary features, now in test mode.
    // After testing should be deleted or moved to main features list.
    public static class Experimental {
        public static final String USE_MULTI_FTGS = "multiftgs";
        public static final String USE_AGGREGATE_DISTINCT = "aggdistinct";
        public static final String CONSISTENT_TIME_BUCKETS = "consistenttime";

        private Experimental() {
        }
    }

    public static boolean includeInCacheKey(final String option) {
        return Experimental.CONSISTENT_TIME_BUCKETS.equals(option);
    }

    private QueryOptions() {
    }
}

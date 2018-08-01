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
 * @author jwolfe
 */
public enum TimeUnit {

    SECOND(1000L, "yyyy-MM-dd HH:mm:ss"),
    MINUTE(1000L * 60, "yyyy-MM-dd HH:mm"),
    HOUR(1000L * 60 * 60, "yyyy-MM-dd HH"),
    DAY(1000L * 60 * 60 * 24, "yyyy-MM-dd"),
    WEEK(1000L * 60 * 60 * 24 * 7, "yyyy-MM-dd"),
    MONTH(TimeUnit.DAY.millis, "MMMM yyyy");

    public final long millis;
    public final String formatString;

    TimeUnit(long millis, String formatString) {
        this.millis = millis;
        this.formatString = formatString;
    }
}

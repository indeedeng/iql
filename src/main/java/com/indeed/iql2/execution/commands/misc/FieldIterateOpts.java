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

package com.indeed.iql2.execution.commands.misc;

import com.indeed.iql2.execution.AggregateFilter;

import java.util.Optional;

public class FieldIterateOpts {
    public Optional<Integer> limit = Optional.empty();
    public Optional<TopK> topK = Optional.empty();
    public Optional<AggregateFilter> filter = Optional.empty();
    public Optional<long[]> sortedIntTermSubset = Optional.empty();
    public Optional<String[]> sortedStringTermSubset = Optional.empty();
}

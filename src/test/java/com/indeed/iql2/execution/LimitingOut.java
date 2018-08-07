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

import com.indeed.iql2.execution.compat.Consumer;

/**
 * @author jwolfe
 */

public class LimitingOut<T> implements Consumer<T> {
    private final Consumer<T> out;
    private final int limit;
    private int seen = 0;

    public LimitingOut(final Consumer<T> out, final int limit) {
        this.out = out;
        this.limit = limit;
    }

    @Override
    public void accept(final T s) {
        if (seen < limit) {
            out.accept(s);
            seen += 1;
        }
    }
}

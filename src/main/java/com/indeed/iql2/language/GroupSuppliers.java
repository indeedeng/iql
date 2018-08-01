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

package com.indeed.iql2.language;

import java.util.Stack;

public class GroupSuppliers {
    public static GroupSupplier newGroupSupplier(final int start) {
        return new GroupSupplier() {
            final Stack<Integer> repushed = new Stack<>();
            int next = start;

            @Override
            public int acquire() {
                if (repushed.isEmpty()) {
                    final int result = next;
                    next += 1;
                    return result;
                } else {
                    return repushed.pop();
                }
            }

            @Override
            public void release(int group) {
                repushed.push(group);
            }
        };
    }
}

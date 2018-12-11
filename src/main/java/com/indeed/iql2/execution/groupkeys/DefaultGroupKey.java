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

package com.indeed.iql2.execution.groupkeys;

import com.indeed.iql2.execution.Session;

public class DefaultGroupKey extends GroupKey {
    public static final DefaultGroupKey DEFAULT_INSTANCE = new DefaultGroupKey("DEFAULT");

    private final String name;

    private DefaultGroupKey(String name) {
        this.name = name;
    }

    public static DefaultGroupKey create(String defaultGroupName) {
        if (defaultGroupName.equals("DEFAULT")) {
            return DEFAULT_INSTANCE;
        } else {
            return new DefaultGroupKey(Session.tsvEscape(defaultGroupName));
        }
    }

    @Override
    public String render() {
        return name;
    }

    @Override
    public boolean isDefault() {
        return true;
    }

    @Override
    public String toString() {
        return "DefaultGroupKey{" +
                "name='" + name + '\'' +
                '}';
    }
}

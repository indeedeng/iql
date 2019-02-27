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

package com.indeed.iql2.sqltoiql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class IQLGroupByQueryPart implements IQLQueryPart  {
    private static final String COMMAND_NAME = "group by";
    private final ImmutableList<IQLGroupByQueryStatement> iqlGroupByQueryStatements;

    public IQLGroupByQueryPart(final ImmutableList<IQLGroupByQueryStatement> iqlGroupByQueryStatements){
        this.iqlGroupByQueryStatements=iqlGroupByQueryStatements;
    }

    @Override
    public String toString() {
        if(!iqlGroupByQueryStatements.isEmpty()) {
            return COMMAND_NAME + " " + Joiner.on(", ").join(iqlGroupByQueryStatements);
        }
            return "";
    }
}

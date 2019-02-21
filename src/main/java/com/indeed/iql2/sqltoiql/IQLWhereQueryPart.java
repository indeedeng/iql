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

public class IQLWhereQueryPart implements IQLQueryPart {

    private static final String COMMAND_NAME = "where";
    private final ImmutableList<IQLWhereQueryStatement> iqlWhereQueryStatements;

    public IQLWhereQueryPart(final ImmutableList<IQLWhereQueryStatement> iqlWhereQueryStatements) {
        this.iqlWhereQueryStatements = iqlWhereQueryStatements;
    }

    @Override
    public String toString() {
        if(!iqlWhereQueryStatements.isEmpty()){
            return COMMAND_NAME + " " + Joiner.on(" AND ").join(iqlWhereQueryStatements);
        }
        else {
            return "";
        }
    }
}

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
 package com.indeed.iql.language;

/**
 * @author vladimir
 */

public class DescribeStatement extends IQLStatement {
    public final String dataset;
    public final String field;

    public DescribeStatement(String dataset, String field) {
        this.dataset = dataset;
        this.field = field;
    }


    @Override
    public String getStatementType() {
        return "describe";
    }
}

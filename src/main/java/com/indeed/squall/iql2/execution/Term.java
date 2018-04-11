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

package com.indeed.squall.iql2.execution;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.log4j.Logger;

/**
 * @author jwolfe
 */
public class Term {
    private static final Logger log = Logger.getLogger(Term.class);

    public final boolean isIntTerm;
    public final String stringTerm;
    public final long intTerm;

    public Term(boolean isIntTerm, String stringTerm, long intTerm) {
        this.isIntTerm = isIntTerm;
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
    }

    public static Term fromJson(JsonNode node) {
        switch (node.get("type").textValue()) {
            case "string":
                return new Term(false, node.get("value").textValue(), 0);
            case "int":
                return new Term(true, null, node.get("value").longValue());
        }
        throw new RuntimeException("Oops: " + node);
    }
}

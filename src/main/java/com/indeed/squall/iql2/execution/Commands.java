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

import com.google.common.base.Optional;
import com.indeed.flamdex.query.Term;

import java.util.List;

/**
 * @author jwolfe
 */
public class Commands {
    public static class TermsWithExplodeOpts {
        public final List<Term> terms;
        public final Optional<String> defaultName;

        public TermsWithExplodeOpts(List<Term> terms, Optional<String> defaultName) {
            this.terms = terms;
            this.defaultName = defaultName;
        }
    }

}

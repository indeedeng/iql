/*
 * Copyright (C) 2014 Indeed Inc.
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
 /*****************************************************************************
 * Copyright (C) Codehaus.org                                                *
 * ------------------------------------------------------------------------- *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 *                                                                           *
 * http://www.apache.org/licenses/LICENSE-2.0                                *
 *                                                                           *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 *****************************************************************************/
package com.indeed.imhotep.sql.ast;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A function call.
 * 
 * @author Ben Yu
 */
public final class FunctionExpression extends ValueObject implements Expression {
  public final String function;
  public final List<Expression> args;
  
  public FunctionExpression(String function, List<Expression> args) {
    this.function = function;
    this.args = Collections.unmodifiableList(args);
  }
  
  public static FunctionExpression of(String function, Expression... args) {
    return new FunctionExpression(function, Arrays.asList(args));
  }

    public <Z> Z match(final Matcher<Z> matcher) {
        return matcher.functionExpression(function, args);
    }
}

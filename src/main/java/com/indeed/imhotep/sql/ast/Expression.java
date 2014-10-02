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

import java.util.List;

/**
 * Any expression
 * 
 * @author Ben Yu
 */
public interface Expression {
    public <Z> Z match(Matcher<Z> matcher);

    public static abstract class Matcher<Z> {

        protected Z binaryExpression(Expression left, Op op, Expression right) {
            return otherwise();
        }

        protected Z unaryExpression(Op op, Expression operand) {
            return otherwise();
        }

        protected Z functionExpression(String name, List<Expression> args) {
            return otherwise();
        }

        protected Z nameExpression(String name) {
            return otherwise();
        }

        protected Z numberExpression(String value) {
            return otherwise();
        }

        protected Z stringExpression(String value) {
            return otherwise();
        }

        protected Z tupleExpression(List<Expression> values) {
            return otherwise();
        }

        protected Z bracketsExpression(String field, String content) {
            return otherwise();
        }

        protected Z otherwise() {
            throw new UnsupportedOperationException();
        }
    }
}

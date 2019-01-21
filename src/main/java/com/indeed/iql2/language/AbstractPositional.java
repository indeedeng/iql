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

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

public abstract class AbstractPositional implements Positional {
    // TODO: Change this to be an Interval.
    private Token start;
    private Token end;
    private ParserRuleContext parserRuleContext = null;

    public AbstractPositional() {
    }

    @Override
    public Token getStart() {
        return start;
    }

    @Override
    public Token getEnd() {
        return end;
    }

    @Override
    public ParserRuleContext getParserRuleContext() {
        return parserRuleContext;
    }

    public void setPosition(Token start, Token end) {
        this.start = start;
        this.end = end;
    }

    public Object copyPosition(ParserRuleContext parserRuleContext) {
        this.setPosition(parserRuleContext.start, parserRuleContext.stop);
        this.parserRuleContext = parserRuleContext;
        return this;
    }

    public Object copyPosition(Positional positional) {
        this.setPosition(positional.getStart(), positional.getEnd());
        return this;
    }
}

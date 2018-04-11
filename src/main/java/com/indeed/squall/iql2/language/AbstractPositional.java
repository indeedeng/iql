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

package com.indeed.squall.iql2.language;

import org.antlr.v4.runtime.ParserRuleContext;

public abstract class AbstractPositional implements Positional {
    // TODO: Change this to be an Interval.
    private Position start;
    private Position end;
    private ParserRuleContext parserRuleContext = null;

    public AbstractPositional() {
    }

    @Override
    public Position getStart() {
        return start;
    }

    @Override
    public Position getEnd() {
        return end;
    }

    @Override
    public ParserRuleContext getParserRuleContext() {
        return parserRuleContext;
    }

    public void setPosition(Position start, Position end) {
        this.start = start;
        this.end = end;
    }

    public void copyPosition(ParserRuleContext parserRuleContext) {
        this.setPosition(Position.from(parserRuleContext.start), Position.from(parserRuleContext.stop));
        this.parserRuleContext = parserRuleContext;
    }

    public void copyPosition(Positional positional) {
        this.setPosition(positional.getStart(), positional.getEnd());
    }
}

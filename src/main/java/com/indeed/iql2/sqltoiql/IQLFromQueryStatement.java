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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;

public class IQLFromQueryStatement {

    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("\"yyyy-MM-dd HH:mm:ss\"");

    private final String identifier;
    private final LocalDateTime startTime;
    private final LocalDateTime endTime;
    private final Optional<String> alias;

    private IQLFromQueryStatement(
            final String identifier,
            final LocalDateTime startTime,
            final LocalDateTime endTime,
            final Optional<String> alias
    ) {
        this.identifier = identifier;
        this.startTime = startTime;
        this.endTime = endTime;
        this.alias = alias;
    }

    @Override
    public String toString() {
        final String prefix =
                Joiner.on(" ")
                        .join(Arrays.asList(identifier, startTime.format(dateFormatter), endTime.format(dateFormatter)));
        return alias.map(s -> prefix + " as " + s).orElse(prefix);
    }

    public static class Builder {
        private final String identifier;
        private Optional<LocalDateTime> startTime = Optional.empty();
        private Optional<LocalDateTime> endTime = Optional.empty();
        private Optional<String> alias = Optional.empty();

        public Builder(final String identifier) {
            this.identifier = identifier;
        }

        public IQLFromQueryStatement build() {
            if (!startTime.isPresent() || !endTime.isPresent()) {
                throw new IQLFromQueryStatementBuildException(this.identifier);
            }

            if (this.startTime.get().isAfter(this.endTime.get())) {
                return new IQLFromQueryStatement(this.identifier, this.endTime.get(), this.startTime.get(), this.alias);
            }
            return new IQLFromQueryStatement(this.identifier, this.startTime.get(), this.endTime.get(), this.alias);
        }

        public Builder addTime(final LocalDateTime newTime) {
            if (this.startTime.isPresent()) {
                this.endTime = Optional.of(newTime);
            } else {
                this.startTime = Optional.of(newTime);
            }
            return this;
        }

        public Builder setAlias(final String alias) {
            this.alias = Optional.of(alias);
            return this;
        }

        public Optional<String> getAlias() {
            return this.alias;
        }
        public Optional<LocalDateTime> getStartTime() {
            return this.startTime;
        }
        public Optional<LocalDateTime> getEndTime() {
            return this.endTime;
        }

    }

    public static class IQLFromQueryStatementBuildException extends RuntimeException {
        public IQLFromQueryStatementBuildException(final String identifier) {
            super("IQLFromQueryStatement requires a start and end date for identifier: "+identifier);
        }
    }
}

package com.indeed.iql.SQLToIQL;


import com.google.common.base.Joiner;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;

public class IQLFromQueryStatement {

    private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("\"yyyy-MM-dd HH:mm:ss\"");

    private final String identifier;
    private final LocalDateTime startTime;
    private final LocalDateTime endTime;
    private Optional<String> alias;

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

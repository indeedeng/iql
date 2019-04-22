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
 package com.indeed.iql1.ez;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.QueryMessage;
import org.apache.commons.codec.binary.Base64;

import java.util.Arrays;
import java.util.List;

/**
 * @author jwolfe
 */
public class Stats {
    private Stats() {
    }

    public abstract static class Stat {
        protected abstract List<String> pushes();
    }

    public static class IntFieldStat extends Stat {
        private final String fieldName;
        IntFieldStat(String fieldName) {
            this.fieldName = fieldName;
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList(fieldName);
        }
        @Override
        public String toString() {
            return "int:"+fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    static void requireValid(StatReference ref) {
        if (!ref.isValid()) {
            throw new IllegalArgumentException("Stat reference is no longer valid!");
        }
    }

    static class BinOpStat extends Stat {
        private final String op;
        private final List<Stat> stats;

        public BinOpStat(String op, Stat... stats) {
            this.op = op;
            this.stats = Arrays.asList(stats);
            for(Stat stat : stats) {
                // TODO proper separation between client side and server side operations
                if(stat instanceof AggregateBinOpStat) {
                    throw new UnsupportedOperationException("Result of aggregate operations like / can't be used as input for further calculations");
                }
            }
        }

        @Override
        protected List<String> pushes() {
            boolean first = true;
            final List<String> ret = Lists.newArrayList();
            for (Stat stat : stats) {
                ret.addAll(stat.pushes());
                if (!first) {
                    ret.add(op);
                }
                first = false;
            }
            return ret;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            boolean first = true;
            for (Stat stat : stats) {
                if (!first) {
                    sb.append(' ').append(op).append(' ');
                }
                sb.append(stat.toString());
                first = false;
            }
            sb.append(')');
            return sb.toString();
        }
    }

    static class AggregateBinOpStat extends Stat {
        private final String op;
        final Stat statLeft;
        final Stat statRight;

        public AggregateBinOpStat(String op, Stat statLeft, Stat statRight) {
            this.op = op;
            this.statLeft = statLeft;
            this.statRight = statRight;
        }

        @Override
        protected List<String> pushes() {
            return Lists.newArrayList(Iterables.concat(statLeft.pushes(), statRight.pushes()));
        }

        @Override
        public String toString() {
            return "(" + statLeft.toString() + ") " + op + " (" + statRight.toString() + ")";
        }
    }

    static class AggregateBinOpConstStat extends Stat {
        private final String op;
        private final long value;
        final Stat statLeft;

        public AggregateBinOpConstStat(String op, Stat statLeft, long value) {
            this.op = op;
            this.statLeft = statLeft;
            this.value = value;
        }

        @Override
        protected List<String> pushes() {
            return statLeft.pushes();
        }

        @Override
        public String toString() {
            return "(" + statLeft.toString() + ") " + op + " " + value;
        }

        public long getValue() {
            return value;
        }

        public String getOp() {
            return op;
        }
    }

    static class ConstantStat extends Stat {
        private final long value;
        public ConstantStat(long value) {
            this.value = value;
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList(Long.toString(value));
        }
        @Override
        public String toString() {
            return Long.toString(value);
        }

        public long getValue() {
            return value;
        }
    }

    static class ExpStat extends Stat {
        private final Stat stat;
        private final int scaleFactor;
        public ExpStat(Stat stat, int scaleFactor) {
            this.stat = stat;
            this.scaleFactor = scaleFactor;
        }
        @Override
        protected List<String> pushes() {
            List<String> prev = Lists.newArrayList(stat.pushes());
            prev.add("exp " + scaleFactor);
            return prev;
        }
        @Override
        public String toString() {
            return "exp("+stat.toString()+", " + scaleFactor + ")";
        }
    }

    static class LogStat extends Stat {
        private final Stat stat;
        private final int scaleFactor;
        public LogStat(Stat stat, int scaleFactor) {
            this.stat = stat;
            this.scaleFactor = scaleFactor;
        }
        @Override
        protected List<String> pushes() {
            List<String> prev = Lists.newArrayList(stat.pushes());
            prev.add("log " + scaleFactor);
            return prev;
        }
        @Override
        public String toString() {
            return "log("+stat.toString()+", " + scaleFactor + ")";
        }
    }

    static class HasIntStat extends Stat {
        private final String field;
        private final long value;
        public HasIntStat(String field, long value) {
            this.field = field;
            this.value = value;
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList("hasint " + field + ":" + Long.toString(value));
        }
        @Override
        public String toString() {
            return "hasint:"+ field + ":" + Long.toString(value);
        }
    }

    static class HasStringStat extends Stat {
        private final String field;
        private final String value;
        public HasStringStat(String field, String value) {
            this.field = field;
            this.value = value;
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList("hasstr " + field + ":" + value);
        }
        @Override
        public String toString() {
            return "hasstr:" + field + ":" + value;
        }
    }

    static class HasStringFieldStat extends Stat {
        private final String field;
        public HasStringFieldStat(String field) {
            this.field = field;
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList("hasstrfield " + field);
        }
        @Override
        public String toString() {
            return "hasstrfield:" + field;
        }
    }

    static class HasIntFieldStat extends Stat {
        private final String field;
        public HasIntFieldStat(String field) {
            this.field = field;
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList("hasintfield " + field);
        }
        @Override
        public String toString() {
            return "hasintfield:" + field;
        }
    }

    static class LuceneQueryStat extends Stat {
        // Query as it is in original query
        private final String queryAsString;
        private final Query luceneQuery;
        public LuceneQueryStat(final String queryAsString, final Query luceneQuery) {
            this.queryAsString = queryAsString;
            this.luceneQuery = luceneQuery;
        }
        @Override
        protected List<String> pushes() {
            final QueryMessage luceneQueryMessage = ImhotepClientMarshaller.marshal(luceneQuery);
            final String base64EncodedQuery = Base64.encodeBase64String(luceneQueryMessage.toByteArray());
            return Lists.newArrayList("lucene " + base64EncodedQuery);
        }
        @Override
        public String toString() {
            return "lucene(" + luceneQuery + ")";
        }
    }

    static class CountStat extends Stat {
        CountStat() {
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList("count()");
        }
        @Override
        public String toString() {
            return "count()";
        }
    }

    public static class CachedStat extends Stat {
        private final Stat stat;
        CachedStat(Stat stat) {
            this.stat = stat;
        }
        @Override
        protected List<String> pushes() {
            List<String> ret = Lists.newArrayList(stat.pushes());
            ret.add("cached()");
            return ret;
        }
    }

    public static class AbsoluteValueStat extends Stat {
        private final Stat stat;
        AbsoluteValueStat(Stat stat) {
            this.stat = stat;
        }
        @Override
        protected List<String> pushes() {
            List<String> ret = Lists.newArrayList(stat.pushes());
            ret.add("abs()");
            return ret;
        }
    }

    public static class FloatScaleStat extends Stat {
        private final String fieldName;
        private final long mult;
        private final long add;

        FloatScaleStat(String fieldName, long mult, long add) {
            this.fieldName = fieldName;
            this.mult = mult;
            this.add = add;
        }
        @Override
        protected List<String> pushes() {
            return Lists.newArrayList("floatscale "+fieldName+" * "+mult+" + "+add);
        }
    }

    public static class MultiplyShiftRight extends Stat {
        private final int shift;
        private final Stat stat1;
        private final Stat stat2;

        MultiplyShiftRight(int shift, Stat stat1, Stat stat2) {
            this.shift = shift;
            this.stat1 = stat1;
            this.stat2 = stat2;
        }
        @Override
        protected List<String> pushes() {
            List<String> ret = Lists.newArrayList(stat1.pushes());
            ret.addAll(stat2.pushes());
            ret.add("mulshr " + shift);
            return ret;
        }
    }

    public static class ShiftLeftDivide extends Stat {
        private final int shift;
        private final Stat stat1;
        private final Stat stat2;

        ShiftLeftDivide(int shift, Stat stat1, Stat stat2) {
            this.shift = shift;
            this.stat1 = stat1;
            this.stat2 = stat2;
        }
        @Override
        protected List<String> pushes() {
            List<String> ret = Lists.newArrayList(stat1.pushes());
            ret.addAll(stat2.pushes());
            ret.add("shldiv " + shift);
            return ret;
        }
    }
}

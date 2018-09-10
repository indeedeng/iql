package com.indeed.iql.marshal;

import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;

// This code is here because of development convenience
// Most likely it should be part of imhotep-client package
// TODO: move this code to Imhotep after testing.
public final class ImhotepMarshallerInIQL {

    public static class FieldOptions {
        public final String field;
        public final boolean intType;
        public final boolean inequality;

        public FieldOptions(
                final String field,
                final boolean intType,
                final boolean inequality) {
            this.field = field;
            this.intType = intType;
            this.inequality = inequality;
        }
    }

    public static class SingleFieldMultiRemapRule {
        public final int targetGroup;
        public final int negativeGroup;
        public final int[] positiveGroups;
        public final long[] intTerms;
        public final String[] stringTerms;

        public SingleFieldMultiRemapRule(
                final int targetGroup,
                final int negativeGroup,
                final int[] positiveGroups,
                final long[] intTerms,
                final String[] stringTerms) {
            if ((intTerms != null) == (stringTerms != null)) {
                throw new IllegalArgumentException();
            }
            final int length = (intTerms != null) ? intTerms.length : stringTerms.length;
            if (length != positiveGroups.length) {
                throw new IllegalArgumentException("positiveGroups.length must equal conditions.length");
            }
            if (stringTerms != null) {
                for (final String term : stringTerms) {
                    if (term == null) {
                        throw new IllegalArgumentException("cannot have null string term");
                    }
                }
            }
            this.targetGroup = targetGroup;
            this.positiveGroups = positiveGroups;
            this.negativeGroup = negativeGroup;
            this.intTerms = intTerms;
            this.stringTerms = stringTerms;
        }
    }

    public static GroupMultiRemapMessage[] marshal(final SingleFieldMultiRemapRule[] rules, final FieldOptions options) {
        final GroupMultiRemapMessage[] result = new GroupMultiRemapMessage[rules.length];
        for (int i = 0; i < rules.length; i++) {
            result[i] = marshal(rules[i], options);
        }
        return result;
    }

    public static GroupMultiRemapMessage marshal(final SingleFieldMultiRemapRule rule, final FieldOptions options) {
        final GroupMultiRemapMessage.Builder builder = GroupMultiRemapMessage.newBuilder();
        builder.setNegativeGroup(rule.negativeGroup).setTargetGroup(rule.targetGroup);
        final int numConditions = rule.positiveGroups.length;
        for (int conditionIx = 0; conditionIx < numConditions; conditionIx++) {
            builder.addCondition(marshal(rule, options, conditionIx));
            builder.addPositiveGroup(rule.positiveGroups[conditionIx]);
        }
        return builder.build();
    }

    public static RegroupConditionMessage marshal(
            final SingleFieldMultiRemapRule rule,
            final FieldOptions options,
            final int termIndex) {
        final RegroupConditionMessage.Builder builder = RegroupConditionMessage.newBuilder()
                .setField(options.field)
                .setIntType(options.intType)
                .setInequality(options.inequality);
        if (options.intType) {
            builder.setIntTerm(rule.intTerms[termIndex]);
        } else {
            builder.setStringTerm(rule.stringTerms[termIndex]);
        }
        return builder.build();
    }
}

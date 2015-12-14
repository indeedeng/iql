package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Commands;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.DumbGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.DumbGroupKeySet;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplodePerGroup implements Command {
    public final List<Commands.TermsWithExplodeOpts> termsWithExplodeOpts;

    public ExplodePerGroup(List<Commands.TermsWithExplodeOpts> termsWithExplodeOpts) {
        this.termsWithExplodeOpts = termsWithExplodeOpts;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        checkNumGroups(session);

        session.timer.push("form rules");
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[session.numGroups];
        int nextGroup = 1;
        final List<GroupKey> nextGroupKeys = Lists.newArrayList((GroupKey) null);
        final IntList nextGroupParents = new IntArrayList();
        nextGroupParents.add(-1);

        final Map<String, GroupKey> stringTermGroupKeys = new HashMap<>();

        for (int group = 1; group <= session.numGroups; group++) {
            final Commands.TermsWithExplodeOpts termsWithExplodeOpts = this.termsWithExplodeOpts.get(group);

            final List<RegroupCondition> regroupConditionsList = Lists.newArrayList();

            final List<Term> terms = termsWithExplodeOpts.terms;
            if (terms.isEmpty() && !termsWithExplodeOpts.defaultName.isPresent()) {
                rules[group - 1] = new GroupMultiRemapRule(group, 0, new int[]{0}, new RegroupCondition[]{new RegroupCondition("fake", true, 152, null, false)});
                continue;
            }

            for (final Term term : terms) {
                if (term.isIntField()) {
                    regroupConditionsList.add(new RegroupCondition(term.getFieldName(), true, term.getTermIntVal(), null, false));
                    nextGroupKeys.add(new IntTermGroupKey(term.getTermIntVal()));
                    nextGroupParents.add(group);
                } else {
                    regroupConditionsList.add(new RegroupCondition(term.getFieldName(), false, 0, term.getTermStringVal(), false));
                    if (!stringTermGroupKeys.containsKey(term.getTermStringVal())) {
                        stringTermGroupKeys.put(term.getTermStringVal(), new DumbGroupKey(term.getTermStringVal()));
                    }
                    nextGroupKeys.add(stringTermGroupKeys.get(term.getTermStringVal()));
                    nextGroupParents.add(group);
                }
            }

            final int[] positiveGroups = new int[regroupConditionsList.size()];
            for (int j = 0; j < regroupConditionsList.size(); j++) {
                positiveGroups[j] = nextGroup++;
            }
            final RegroupCondition[] conditions = regroupConditionsList.toArray(new RegroupCondition[regroupConditionsList.size()]);
            final int negativeGroup;
            if (termsWithExplodeOpts.defaultName.isPresent()) {
                negativeGroup = nextGroup++;
                // TODO: Memoize this?
                nextGroupKeys.add(new DumbGroupKey(termsWithExplodeOpts.defaultName.get()));
                nextGroupParents.add(group);
            } else {
                negativeGroup = 0;
            }
            rules[group - 1] = new GroupMultiRemapRule(group, negativeGroup, positiveGroups, conditions);
        }
        session.timer.pop();

        session.timer.push("regroup");
        // TODO: Parallelize
        for (final ImhotepSession s : session.getSessionsMapRaw().values()) {
            s.regroup(rules);
        }
        session.timer.pop();

        session.numGroups = nextGroup - 1;
        session.groupKeySet = DumbGroupKeySet.create(session.groupKeySet, nextGroupParents.toIntArray(), nextGroupKeys);
        session.currentDepth += 1;

        out.accept("success");
    }

    private void checkNumGroups(Session session) {
        int numGroups = 0;
        for (final Commands.TermsWithExplodeOpts termsWithExplodeOpt : this.termsWithExplodeOpts) {
            if (termsWithExplodeOpt != null) {
                numGroups += termsWithExplodeOpt.terms.size();
                if (termsWithExplodeOpt.defaultName.isPresent()) {
                    numGroups += 1;
                }
            }
        }
        session.checkGroupLimit(numGroups);
    }
}

package com.indeed.jql.language.commands;

public class RegroupIntoParent implements Command {
    private final GroupLookupMergeType mergeType;

    public RegroupIntoParent(GroupLookupMergeType mergeType) {
        this.mergeType = mergeType;
    }

    @Override
    public String toString() {
        return "RegroupIntoParent{" +
                "mergeType=" + mergeType +
                '}';
    }
}

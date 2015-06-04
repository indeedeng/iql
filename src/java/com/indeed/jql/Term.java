package com.indeed.jql;

public class Term {
    public final String stringTerm;
    public final long intTerm;
    public final boolean isIntTerm;

    private Term(String stringTerm, long intTerm, boolean isIntTerm) {
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
        this.isIntTerm = isIntTerm;
    }

    public static Term term(String term) {
        return new Term(term, 0, false);
    }

    public static Term term(long term) {
        return new Term(null, term, true);
    }
}

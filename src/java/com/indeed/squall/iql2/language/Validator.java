package com.indeed.squall.iql2.language;

public interface Validator {
    void error(String error);
    void warn(String warn);
}

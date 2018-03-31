package com.indeed.squall.iql2.language.actions;

import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

public interface Action {
    void validate(ValidationHelper validationHelper, Validator validator);
}

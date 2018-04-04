package com.indeed.squall.iql2.language.commands;

import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

public interface Command {
    // TODO: Clean up this API
    void validate(
            ValidationHelper validationHelper,
            Validator validator
    );
}

package com.indeed.squall.iql2.language.actions;

import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.DatasetsFields;

public interface Action {
    void validate(DatasetsFields datasetsFields, Validator validator);
}

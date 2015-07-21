package com.indeed.squall.iql2.language.actions;

import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.util.Map;
import java.util.Set;

public interface Action {
    void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer);
}

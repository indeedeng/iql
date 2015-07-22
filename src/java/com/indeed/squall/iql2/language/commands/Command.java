package com.indeed.squall.iql2.language.commands;

import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

public interface Command {
    // TODO: Clean up this API
    void validate(
            DatasetsFields datasetsFields,
            Consumer<String> errorConsumer
    );
}

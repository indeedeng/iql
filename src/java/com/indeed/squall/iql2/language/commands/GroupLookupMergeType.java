package com.indeed.squall.iql2.language.commands;

import com.fasterxml.jackson.databind.JsonNode;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.util.DatasetsFields;

public enum GroupLookupMergeType implements Command {
    SumAll,
    TakeTheOneUniqueValue,
    FailIfPresent;

    public static GroupLookupMergeType parseJson(JsonNode jsonNode) {
        if (!jsonNode.isTextual()) {
            throw new IllegalArgumentException("GroupLookupMergeType.parseJson argument must be a text node!");
        }
        final String jsonText = jsonNode.textValue();
        for (final GroupLookupMergeType value : GroupLookupMergeType.values()) {
            if (value.name().equalsIgnoreCase(jsonText)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown GroupLookupMergeType: [" + jsonText + "]");
    }

    @Override
    public void validate(DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

    }
}

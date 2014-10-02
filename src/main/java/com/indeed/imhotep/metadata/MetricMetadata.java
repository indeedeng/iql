package com.indeed.imhotep.metadata;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.codehaus.jackson.node.ObjectNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author vladimir
 */

public class MetricMetadata {
    @Nonnull final String name;
    @Nullable String friendlyName;
    @Nullable String description;
    @Nullable String expression;
    @Nullable String unit;
    boolean isHidden = false;

    public MetricMetadata(@Nonnull String name) {
        Preconditions.checkNotNull(name);
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nullable
    public String getFriendlyName() {
        return friendlyName;
    }

    public void setFriendlyName(@Nullable String friendlyName) {
        this.friendlyName = friendlyName;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    @Nullable
    public String getExpression() {
        return expression;
    }

    public void setExpression(@Nullable String expression) {
        this.expression = expression;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        isHidden = hidden;
    }

    /**
     * Returns a unit of measurement for the metric (e.g. clicks, hours, etc.)
     */
    @Nullable
    public String getUnit() {
        return unit;
    }

    public void setUnit(@Nullable String unit) {
        this.unit = unit;
    }

    public void toJSON(ObjectNode jsonNode) {
        jsonNode.put("name", getName());
        final String description = Strings.nullToEmpty(getDescription());
        jsonNode.put("description", description);
        String unit = getUnit();
        if(unit != null) {
            jsonNode.put("unit", unit);
        }
        final String expression = getExpression();
        if(expression != null) {
            jsonNode.put("expression", expression);
        }
    }
}

package com.indeed.iql2;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * @author jwolfe
 */
public class IQL2Options {
    private final Set<String> options = Sets.newHashSet();

    public ImmutableSet<String> getOptions() {
        return ImmutableSet.copyOf(options);
    }

    public void addOptions(Set<String> additionalOptions) {
        options.addAll(additionalOptions);
    }
}

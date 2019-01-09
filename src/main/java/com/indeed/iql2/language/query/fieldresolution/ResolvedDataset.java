package com.indeed.iql2.language.query.fieldresolution;

import com.google.common.collect.Iterables;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetMetadata;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author jwolfe
 */
class ResolvedDataset {
    // Name as referenced in the query
    final String name;
    // Name as per imhotep's definition of it
    final String imhotepName;
    // Outer key is case insensitive field name.
    // Inner key is case sensitive field name.
    // Inner value is case correct aliased field (real imhotep field name).
    final Map<String, Map<String, String>> fieldAliasEquivalenceSets;
    final DatasetMetadata datasetMetadata;

    ResolvedDataset(final String name, final String imhotepName, final Map<String, String> fieldAliases, final DatasetMetadata datasetMetadata) {
        this.name = name;
        this.imhotepName = imhotepName;

        fieldAliasEquivalenceSets = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (final Map.Entry<String, String> entry : fieldAliases.entrySet()) {
            final String aliasName = entry.getKey();
            fieldAliasEquivalenceSets
                    .computeIfAbsent(aliasName, x -> new HashMap<>())
                    .put(entry.getKey(), entry.getValue());
        }

        this.datasetMetadata = datasetMetadata;
    }

    // Null indicates there exists no alias of this name (of exact or inexact case match)
    @Nullable
    private String resolveQueryFieldAlias(final String alias) {
        final Map<String, String> equivalent = fieldAliasEquivalenceSets.get(alias);

        // No alias exists
        if (equivalent == null) {
            return null;
        }

        // Prefer exact matched alias names
        if (equivalent.containsKey(alias)) {
            return equivalent.get(alias);
        }

        // Fail if ambiguous
        if (equivalent.size() > 1) {
            throw new IqlKnownException.UnknownFieldException("Multiple field aliases match, and none are an exact match: " + equivalent + ", seeking \"" + alias + "\"");
        }

        // Unambiguous, but non-exact match is okay
        return Iterables.getOnlyElement(equivalent.values());
    }

    String resolveFieldName(final String typedName) {
        final String newName = resolveQueryFieldAlias(typedName);
        final String nameToResolve = (newName == null) ? typedName : newName;
        // Resolve whatever we resolved to
        return datasetMetadata.resolveFieldName(nameToResolve);
    }
}

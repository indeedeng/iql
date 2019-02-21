package com.indeed.iql2.language.cachekeys;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.Shard;
import com.indeed.iql.web.SelectQuery;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.execution.ResultFormat;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.Query;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class CacheKey {
    private static Logger log = Logger.getLogger(CacheKey.class);

    public final String rawHash;
    public final String cacheFileName;

    public CacheKey(final String rawHash, final String cacheFileName) {
        this.rawHash = rawHash;
        this.cacheFileName = cacheFileName;
    }

    public static CacheKey computeCacheKey(final Query query, final ResultFormat resultFormat) {
        final List<Command> commands = query.commands();
        final TreeSet<String> sortedOptions = Sets.newTreeSet(query.options);
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed to init SHA1", e);
            throw Throwables.propagate(e);
        }
        sha1.update(Ints.toByteArray(SelectQuery.VERSION_FOR_HASHING));
        sha1.update(Ints.toByteArray(query.useLegacy ? 1 : 2));
        sha1.update(resultFormat.toString().getBytes());
        for (final Command command : commands) {
            sha1.update(command.toString().getBytes(Charsets.UTF_8));
        }
        for (final Dataset dataset : query.datasets) {
            sha1.update(dataset.dataset.unwrap().getBytes(Charsets.UTF_8));
            sha1.update(Longs.toByteArray(dataset.startInclusive.unwrap().getMillis()));
            sha1.update(Longs.toByteArray(dataset.endExclusive.unwrap().getMillis()));
            final List<FieldAlias> sortedFieldAliases = dataset.fieldAliases.entrySet().stream()
                    .map(e -> new FieldAlias(e.getValue().unwrap(), e.getKey().unwrap()))
                    .sorted(Comparator.comparing(alias -> alias.newName))
                    .collect(Collectors.toList());
            for (final FieldAlias fieldAlias : sortedFieldAliases) {
                sha1.update(fieldAlias.toString().getBytes(Charsets.UTF_8));
            }
            if (dataset.shards != null) {
                for (final Shard shard : dataset.shards) {
                    sha1.update(shard.getFileName().getBytes(Charsets.UTF_8));
                    sha1.update(Ints.toByteArray(shard.getNumDocs()));
                }
            }
        }
        for (final String option : sortedOptions) {
            if (QueryOptions.includeInCacheKey(option)) {
                sha1.update(option.getBytes(Charsets.UTF_8));
            }
        }
        sha1.update(Ints.toByteArray(query.rowLimit.or(-1)));
        final String queryHash = Base64.encodeBase64URLSafeString(sha1.digest());
        final String cacheFileName = "IQL2-" + queryHash + ".tsv";
        return new CacheKey(queryHash, cacheFileName);
    }

    @EqualsAndHashCode
    @ToString
    private static class FieldAlias {
        public final String originalName;
        public final String newName;

        FieldAlias(final String originalName, final String newName) {
            this.originalName = originalName;
            this.newName = newName;
        }
    }
}

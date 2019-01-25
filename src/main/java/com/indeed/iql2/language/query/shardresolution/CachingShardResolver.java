package com.indeed.iql2.language.query.shardresolution;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.indeed.imhotep.Shard;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * A cache that is local to an individual query / its subqueries which allows us
 * to guarantee that if the query requests the same (dataset, start, end) for different
 * parts of the query, it will see the same shards no matter what.
 */
public class CachingShardResolver implements ShardResolver {
    // Doesn't outlive a query, doesn't need to be limited in size
    private final LoadingCache<Key, ShardResolutionResult> cache;

    public CachingShardResolver(final ShardResolver inner) {
        cache = CacheBuilder.newBuilder()
                .build(new CacheLoader<Key, ShardResolutionResult>() {
                    @Override
                    public ShardResolutionResult load(final Key key) {
                        return inner.resolve(key.dataset, key.start, key.end);
                    }
                });
    }

    @Nullable
    @Override
    public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
        try {
            return cache.get(new Key(dataset, start, end));
        } catch (final ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    static class Key {
        final String dataset;
        final DateTime start;
        final DateTime end;

        private Key(final String dataset, final DateTime start, final DateTime end) {
            this.dataset = dataset;
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Key key = (Key) o;
            return Objects.equals(dataset, key.dataset) &&
                    Objects.equals(start, key.start) &&
                    Objects.equals(end, key.end);
        }

        @Override
        public int hashCode() {

            return Objects.hash(dataset, start, end);
        }
    }
}

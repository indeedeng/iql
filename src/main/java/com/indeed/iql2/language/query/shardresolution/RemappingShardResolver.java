package com.indeed.iql2.language.query.shardresolution;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.client.Host;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.util.core.Pair;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RemappingShardResolver implements ShardResolver {
    private final static Random RANDOM = new Random(Long.MAX_VALUE);
    private final QueryOptions.HostsMappingMethod method;
    private final ShardResolver wrapped;
    private final List<Host> hostsFromOption;

    public RemappingShardResolver(final ShardResolver wrapped, final QueryOptions.HostsMappingMethod method, final List<Host> hostsFromOption) {
        this.method = method;
        this.wrapped = wrapped;
        this.hostsFromOption = hostsFromOption;
    }

    @Nullable
    @Override
    public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
        final ShardResolutionResult initialResults = wrapped.resolve(dataset, start, end);

        final List<Shard> remappedShards;
        switch (method) {
            case NUMDOC_MAPPING:
                remappedShards = remapShardsByNumDocs(initialResults.shards, hostsFromOption);
                break;

            case SHUFFLE_MAPPING:
            default:
                remappedShards = remapShardsByShuffle(initialResults.shards, hostsFromOption);
                break;
        }

        return new ShardResolutionResult(remappedShards, initialResults.missingShardTimeIntervals);
    }

    private static List<Shard> remapShardsByShuffle(final List<Shard> shards, final List<Host> hosts) {
        Preconditions.checkArgument(!hosts.isEmpty());

        Collections.shuffle(shards, RANDOM);
        return IntStream.range(0, shards.size())
                .mapToObj(shardIndex -> {
                    final Shard shard = shards.get(shardIndex);
                    return shard.withHost(hosts.get(shardIndex % hosts.size()));
                })
                .collect(Collectors.toList());
    }

    // It's a NP-complete K-partition problem: https://en.wikipedia.org/wiki/Partition_problem
    // and we take the greedy approximate algorithm https://en.wikipedia.org/wiki/Partition_problem#The_greedy_algorithm
    private static List<Shard> remapShardsByNumDocs(final List<Shard> shards, final List<Host> hosts) {
        Preconditions.checkArgument(!hosts.isEmpty());

        // sort numdocs in descending order and keep pair<NumDoc, ShardIndex> in the list
        final List<Pair<Integer, Integer>> shardNumDocs = IntStream.range(0, shards.size())
                .mapToObj(i -> Pair.of(shards.get(i).getNumDocs(), i))
                .sorted((s1, s2) -> s2.getFirst().compareTo(s1.getFirst()))
                .collect(Collectors.toList());

        // greedy partition algorithm, keep pair<Sum of numDoc, list of shard indices> in the heap
        final PriorityQueue<Pair<Long, List<Integer>>> docSumQueue = new PriorityQueue<>(
                hosts.size(), Comparator.comparing(Pair::getFirst));
        hosts.forEach(host -> docSumQueue.add(Pair.of(0L, new ArrayList<>())));
        shardNumDocs.forEach(
                numDocPair -> {
                    final Pair<Long, List<Integer>> poll = docSumQueue.poll();
                    poll.getSecond().add(numDocPair.getSecond());
                    docSumQueue.add(Pair.of(poll.getFirst() + numDocPair.getFirst(), poll.getSecond()));
                }
        );

        final List<Shard> remappedShards = Lists.newArrayListWithCapacity(shards.size());
        int hostIndex = 0;
        while (!docSumQueue.isEmpty()) {
            final Pair<Long, List<Integer>> poll = docSumQueue.poll();
            for (final int shardIndex : poll.getSecond()) {
                remappedShards.add(shards.get(shardIndex).withHost(hosts.get(hostIndex)));
            }
            hostIndex++;
        }
        return remappedShards;
    }
}

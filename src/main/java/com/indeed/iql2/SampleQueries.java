package com.indeed.iql2;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class SampleQueries {
    private SampleQueries() {
    }

    public static final int NUM_QUERIES = 200;
    public static final String FILE = "/home/xweng/indeed/data-infra/performance-benchmark/iqlquery_2019-01-01_2019-01-08.tsv";
    public static final String OUT = "/home/xweng/indeed/data-infra/performance-benchmark/sampled.csv";

    public static void main(String[] args) throws IOException {
        long totalCount = 0L;

        final List<Query> queries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE))) {
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    break;
                }

                final String[] columns = line.split("\t");

                Preconditions.checkState(columns.length == 2);

                final String query = columns[0]
                        .replaceAll("�", "\n")
                        .trim()
                        .replaceAll("\\s+", " ");
                final long count = Long.parseLong(columns[1]);

                queries.add(new Query(query, count));

                totalCount += count;
            }
        }

        final long[] cumulativeCounts = new long[queries.size()];

        long countSoFar = 0L;
        for (int i = 0; i < queries.size(); i++) {
            countSoFar += queries.get(i).count;
            cumulativeCounts[i] = countSoFar;
        }

        final Random rng = new Random();
        final Set<String> selectedQueries = new HashSet<>();

        try (final CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(OUT)), ',', '\"', '\\')) {
            final String[] buf = new String[1];
            buf[0] = "query";
            writer.writeNext(buf);
            for (int i = 0; i < NUM_QUERIES; i++) {
                final long targetCount = (long) Math.floor(rng.nextDouble() * totalCount);
                int index = Arrays.binarySearch(cumulativeCounts, targetCount);
                if (index < 0) {
                    index = -(index + 1);
                }
                if (index > cumulativeCounts.length) {
                    i -= 1;
                    continue;
                }
                final Query query = queries.get(index);
                if (selectedQueries.contains(query.query)) {
                    i -= 1;
                    continue;
                }
                selectedQueries.add(query.query);
                buf[0] = URLEncoder.encode(query.query, "UTF-8");
                writer.writeNext(buf);
            }
        }



    }

    private static class Query {
        private final String query;
        private final long count;

        private Query(final String query, final long count) {
            this.query = query;
            this.count = count;
        }
    }
}
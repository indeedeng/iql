package com.indeed.iql2;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.ims.client.ImsClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql2.execution.ResultFormat;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

public class HashQueries {
    private HashQueries() {
    }

    public static void main(final String[] args) throws IOException, URISyntaxException {
        final String zkNodes = Objects.requireNonNull(System.getenv("SHARDMASTER_ZK_NODES"), "Environment variable SHARDMASTER_ZK_NODES must be configured to imhotep zookeeper nodes");
        final String zkPath = Objects.requireNonNull(System.getenv("SHARDMASTER_ZK_PATH"), "Environment variable SHARDMASTER_ZK_PATH must be configured to imhotep zookeeper path");
        final String iqlUrl = Objects.requireNonNull(System.getenv("IQL_URL"), "Environment variable IQL_URL must be configured to the URL at which IQL is available");

        final String inFile = args[0];
        final String outFile = args[1];

        final DatasetsMetadata datasetsMetadata;
        try (final ImhotepClient client = new ImhotepClient(zkNodes, zkPath, true)) {
            final ImhotepMetadataCache metadataCache;
            final ImsClientInterface imsClient = ImsClient.build(iqlUrl);
            metadataCache = new ImhotepMetadataCache(imsClient, client, "", new FieldFrequencyCache(null), true);
            metadataCache.updateDatasets();
            datasetsMetadata = metadataCache.get();
        }
        final WallClock clock = new DefaultWallClock();

        int successes = 0;
        int failures = 0;
        int skippedParentheses = 0;
        int skippedTooLong = 0;
        int skippedWrongLength = 0;
        try (final CSVReader csvReader = new CSVReader(new FileReader(inFile), ',', '"', '\0');
             final CSVWriter csvWriter = new CSVWriter(new FileWriter(outFile), ',', '"', '\\')) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                final String query = line[0];

                if (line.length != 2) {
                    System.out.println("line = " + Arrays.toString(line));
                    skippedWrongLength += 1;
                    continue;
                }

                if (query.length() > 8000) {
                    skippedTooLong += 1;
                    continue;
                }

                if (query.contains("...)")) {
                    skippedParentheses += 1;
                    continue;
                }

                String rawHash;
                try {
                    final Queries.ParseResult parseResult = Queries.parseQuery(
                            query,
                            false,
                            datasetsMetadata,
                            Collections.emptySet(),
                            clock,
                            new TracingTreeTimer(),
                            new NullShardResolver()
                    );
                    rawHash = parseResult.query.cacheKey(ResultFormat.TSV).rawHash;
                    successes += 1;
                } catch (final Exception e) {
                    rawHash = "failed";
                    failures += 1;
                }

                line[1] = rawHash;
                csvWriter.writeNext(line);
            }
        }

        System.out.println("successes = " + successes);
        System.out.println("failures = " + failures);
        System.out.println("skipped (truncated term lists) = " + skippedParentheses);
        System.out.println("skippedTooLong = " + skippedTooLong);
        System.out.println("skippedWrongLength = " + skippedWrongLength);
    }
}

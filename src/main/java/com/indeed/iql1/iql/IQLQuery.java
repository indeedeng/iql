/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.iql1.iql;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.StrictCloser;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.web.Limits;
import com.indeed.iql.web.QueryInfo;
import com.indeed.iql1.ez.EZImhotepSession;
import com.indeed.iql1.ez.GroupKey;
import com.indeed.iql1.ez.StatReference;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.logging.TracingTreeTimer;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.indeed.iql1.ez.Stats.Stat;

/**
 * @author jplaisance
 */
public final class IQLQuery implements Closeable {
    private static final int IN_MEMORY_ROW_LIMIT = 50000;
    private static final Logger log = Logger.getLogger(IQLQuery.class);
    private static final Period executionTimeout = Period.minutes(6);
    public static final String TEMP_FILE_PREFIX = "iql_tmp";
    private static final String EVENT_SOURCE_END = "\n\n";

    private final List<Stat> stats;
    private final String dataset;
    private final DateTime start;
    private final DateTime end;
    private final List<Condition> conditions;
    private final List<Grouping> groupings;
    private final int rowLimit;
    private final List<Shard> shards;
    private final List<Interval> timeIntervalsMissingShards;
    private final ImhotepClient.SessionBuilder sessionBuilder;
    private final long shardsSelectionMillis;
    private final Limits limits;
    private final StrictCloser strictCloser;
    // session used for the current execution
    private EZImhotepSession session;
    private final Set<String> fields;
    private final DatasetMetadata datasetMetadata;
    private final QueryInfo queryInfo;
    private final Set<String> datasetFields;

    public IQLQuery(
            final ImhotepClient client,
            final List<Stat> stats,
            final String dataset,
            final DateTime start,
            final DateTime end,
            final @Nonnull List<Condition> conditions,
            final @Nonnull List<Grouping> groupings,
            final int rowLimit,
            final String username,
            final Limits limits,
            final Set<String> fields,
            final DatasetMetadata datasetMetadata,
            final QueryInfo queryInfo,
            final StrictCloser strictCloser
    ) {
        this.stats = stats;
        this.dataset = dataset;
        this.start = start;
        this.end = end;
        this.conditions = conditions;
        this.groupings = groupings;
        this.rowLimit = rowLimit;
        this.limits = limits;
        this.fields = fields;
        this.datasetMetadata = datasetMetadata;
        this.queryInfo = queryInfo;
        this.datasetFields = fields.stream().map(field -> dataset + "." + field).collect(Collectors.toSet());
        this.strictCloser = strictCloser;

        long shardsSelectionStartTime = System.currentTimeMillis();
        sessionBuilder = client.sessionBuilder(dataset, start, end)
                .localTempFileSizeLimit(mbToBytes(limits.queryFTGSIQLLimitMB))
                .daemonTempFileSizeLimit(mbToBytes(limits.queryFTGSImhotepDaemonLimitMB)).username(username).clientName("IQL");
        shards = sessionBuilder.getChosenShards();
        if ((shards == null) || shards.isEmpty()) {
            throw new IqlKnownException.NoDataException("No shards: no data available for the requested dataset and time range."
                    + " Dataset: " + dataset + ", start: " + start + ", end: " + end);
        }
        shardsSelectionMillis = System.currentTimeMillis() - shardsSelectionStartTime;

        timeIntervalsMissingShards = sessionBuilder.getTimeIntervalsMissingShards();
    }

    private Long mbToBytes(Integer megabytes) {
        if(megabytes == null) {
            return 0L;
        }
        return megabytes <= 0 ? (long)megabytes : (long)megabytes * 1024 * 1024;
    }


    /**
     * Not thread safe due to session reference caching for close().
     */
    public ExecutionResult execute(boolean progress, PrintWriter outputStream) throws ImhotepOutOfMemoryException {
        queryInfo.shardsSelectionMillis = shardsSelectionMillis;
        //if outputStream passed, update on progress
        final PrintWriter out = progress ? outputStream : null;

        try (final TracingTreeTimer timer = new TracingTreeTimer()) {
            timer.push("Imhotep session creation");
            final ImhotepSession imhotepSession = sessionBuilder.build();
            final ImhotepSessionHolder sessionHolder = new ImhotepSessionHolder(dataset, imhotepSession);
            session = new EZImhotepSession(sessionHolder, limits);
            strictCloser.registerOrClose(session);

            final long numDocs = imhotepSession.getNumDocs();
            if (!limits.satisfiesQueryDocumentCountLimit(numDocs)) {
                DecimalFormat df = new DecimalFormat("###,###");
                throw new IqlKnownException.DocumentsLimitExceededException("The query on " + df.format(numDocs) +
                        " documents exceeds the limit of " + df.format(limits.queryDocumentCountLimitBillions * 1_000_000_000L) + ". Please reduce the time range.");
            }
            queryInfo.numDocs = numDocs;
            final String imhotepSessionId = imhotepSession.getSessionId();
            queryInfo.sessionIDs = Collections.singleton(imhotepSessionId);

            queryInfo.createSessionMillis = (long) timer.pop();

            final long timeoutTS = System.currentTimeMillis() + executionTimeout.toStandardSeconds().getSeconds() * 1000;

            try {
                final int steps = conditions.size() + (groupings.size() == 0 ? 1 : groupings.size()) - 1;
                int count = 0;
                if (progress) {
                    out.println(": Beginning IQL Query");
                    out.println("event: totalsteps");
                    out.print("data: " + steps + EVENT_SOURCE_END);
                    out.println("event: sessionid");
                    out.print("data: " + imhotepSessionId + EVENT_SOURCE_END);
                    out.print(": Starting time filter" + EVENT_SOURCE_END);
                    out.flush();
                }
                timer.push("Time filter");
                timeFilter(session);
                queryInfo.timeFilterMillis = (long) timer.pop();
                if (progress) {
                    out.print(": Time filtering finished" + EVENT_SOURCE_END);
                    out.flush();
                }

                long conditionFilterMillis = 0;
                for (int i = 0; i < conditions.size(); i++) {
                    final Condition condition = conditions.get(i);
                    checkTimeout(timeoutTS);
                    timer.push("Filter", "Filter " + (i + 1));
                    condition.filter(session);
                    conditionFilterMillis += timer.pop();
                    count = updateProgress(progress, out, count);
                }
                queryInfo.conditionFilterMillis = conditionFilterMillis;

                queryInfo.maxGroups = 0;
                final ExecutionResult executionResult;

                timer.push("Pushing stats");
                final List<StatReference> statRefs = pushStats(session);
                queryInfo.pushStatsMillis = (long) timer.pop();

                timer.push("Getting totals");
                final double[] totals = getStats(statRefs);
                queryInfo.getStatsMillis = (long) timer.pop();

                if (groupings.size() > 0) {
                    long regroupMillis = 0;
                    Int2ObjectMap<GroupKey> groupKeys = EZImhotepSession.newGroupKeys();
                    // do Imhotep regroup on all except the last grouping
                    for (int i = 0; i < groupings.size() - 1; i++) {
                        checkTimeout(timeoutTS);
                        timer.push("Regroup", "Regroup " + (i + 1));
                        groupKeys = groupings.get(i).regroup(session, groupKeys);
                        queryInfo.maxGroups = Math.max(queryInfo.maxGroups, session.getNumGroups());
                        regroupMillis += timer.pop();
                        count = updateProgress(progress, out, count);
                    }
                    queryInfo.regroupMillis = regroupMillis;
                    checkTimeout(timeoutTS);

                    // do FTGS on the last grouping
                    timer.push("FTGS");
                    final Iterator<GroupStats> groupStatsIterator = groupings.get(groupings.size() - 1).getGroupStats(session, groupKeys, statRefs);
                    if (groupStatsIterator instanceof Closeable) {
                        strictCloser.registerOrClose((Closeable) groupStatsIterator);
                    }
                    queryInfo.maxGroups = Math.max(queryInfo.maxGroups, session.getNumGroups());
                    final long ftgsMillis = timer.pop();
                    queryInfo.ftgsMillis = ftgsMillis;
                    updateProgress(progress, out, count);

                    executionResult = new ExecutionResult(groupStatsIterator, totals);
                } else {
                    count = updateProgress(progress, out, count);
                    final List<GroupStats> result = Lists.newArrayList();
                    result.add(new GroupStats(GroupKey.<Comparable>empty(), totals));
                    executionResult = new ExecutionResult(result.iterator(), totals);
                }
                queryInfo.timingTreeReport = timer.toString();
                queryInfo.ftgsMB = session.getTempFilesBytesWritten() / 1024 / 1024;
                return executionResult;
            } catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, IqlKnownException.class);
                log.error("Error while executing the query", t);
                throw Throwables.propagate(t);
            }
        }
    }

    private int updateProgress(boolean progress, PrintWriter out, int count) {
        count++;
        if(progress) {
            out.println("event: chunkcomplete");
            out.print("data: " + count + EVENT_SOURCE_END);
            out.flush();
        }
        return count;
    }

    private double[] getStats(List<StatReference> statRefs) throws ImhotepOutOfMemoryException {
        final double[] stats = new double[statRefs.size()];
        for (int i = 0; i < statRefs.size(); i++) {
            final double[] groupStat = session.getGroupStats(statRefs.get(i));
            stats[i] = groupStat.length > 1 ? groupStat[1] : 0;
        }
        return stats;
    }

    public static class ExecutionResult {
        private final Iterator<GroupStats> rows;
        private final double[] totals;

        public ExecutionResult(Iterator<GroupStats> rows, double[] totals) {
            this.rows = rows;
            this.totals = totals;
        }

        public Iterator<GroupStats> getRows() {
            return rows;
        }

        public double[] getTotals() {
            return totals;
        }
    }

    private void timeFilter(EZImhotepSession session) throws ImhotepOutOfMemoryException {
        final LongRange shardsMinMax = getShardsMinMax(shards);
        final long min = shardsMinMax.getMin();
        final long max = shardsMinMax.getMax();
        if (min < start.getMillis() || max > end.getMillis()) {
            new MetricCondition(EZImhotepSession.intField(DatasetMetadata.TIME_FIELD_NAME),
                    (int)(start.getMillis()/1000), (int)((end.getMillis()-1)/1000), false).filter(session);
        }
    }

    /**
     * Throws UncheckedTimeoutException if current time is past the provided timeout timestamp.
     * @param timeoutTS timestamp of when the query times out in milliseconds
     */
    public void checkTimeout(long timeoutTS) {
        if(System.currentTimeMillis() > timeoutTS) {
            throw new UncheckedTimeoutException("The query took longer than the allowed timeout of " + executionTimeout.toString(PeriodFormat.getDefault()));
        }
    }

    private static final class LongRange {
        private final long min;
        private final long max;

        private LongRange(final long min, final long max) {
            this.min = min;
            this.max = max;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }
    }

    /**
     * Returns minimum and maximum milliseconds covered by the list of shards
     */
    private static LongRange getShardsMinMax(final List<Shard> shards) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (Shard shard : shards) {
            final ShardInfo.DateTimeRange interval = shard.getRange();
            if (interval.start.getMillis() < min) {
                min = interval.start.getMillis();
            }
            if (interval.end.getMillis() > max) {
                max = interval.end.getMillis();
            }
        }
        return new LongRange(min, max);
    }

    private List<StatReference> pushStats(EZImhotepSession session) throws ImhotepOutOfMemoryException {
        final List<StatReference> statRefs = Lists.newArrayList();
        for (Stat stat : stats) {
            final StatReference statReference = session.pushStatGeneric(stat);
            statRefs.add(statReference);
        }
        return statRefs;
    }

    private boolean requiresSorting() {
        // TODO: enable sorting
//        if(groupings.size() > 0) {
//            final Grouping lastGrouping = groupings.get(groupings.size() - 1);
//            if(lastGrouping instanceof FieldGrouping && ((FieldGrouping)lastGrouping).isNoExplode()) {
//                return true;    // currently we only have to sort when using non-exploded field grouping as the last grouping
//            }
//        }
        return false;
    }

    public static class WriteResults {
        public final int rowsWritten;
        public final File unsortedFile;
        public final Iterator<GroupStats> resultCacheIterator;
        public final boolean exceedsLimit;

        public WriteResults(int rowsWritten, File unsortedFile, Iterator<GroupStats> resultCacheIterator, boolean exceedsLimit) {
            this.rowsWritten = rowsWritten;
            this.unsortedFile = unsortedFile;
            this.resultCacheIterator = resultCacheIterator;
            this.exceedsLimit = exceedsLimit;
        }

        public boolean didOverflowToDisk() {
            return unsortedFile != null;
        }
    }

    @Nonnull
    public WriteResults outputResults(final Iterator<GroupStats> rows, PrintWriter httpOutStream, final boolean csv, final boolean progress, final int rowLimit, int groupingColumns, int selectColumns, boolean cacheDisabled) {
        final long timeStarted = System.currentTimeMillis();
        final boolean requiresSorting = requiresSorting();
        if(cacheDisabled && !requiresSorting) { // just stream the rows out. don't have to worry about keeping a copy at all
            final int rowsWritten = writeRowsToStream(rows, httpOutStream, csv, rowLimit, progress);
            return new WriteResults(rowsWritten, null, null, rows.hasNext());
        }

        List<GroupStats> resultsCache = Lists.newArrayList();

        int rowsLoaded = 0;
        final int limit = Math.min(IN_MEMORY_ROW_LIMIT, rowLimit);
        while(rows.hasNext() &&  rowsLoaded < limit) {
            resultsCache.add(rows.next());
            rowsLoaded++;
        }
        final boolean cacheOverflow = rowsLoaded >= IN_MEMORY_ROW_LIMIT;

        // TODO: figure out the size of the resulting data for reporting or limiting?
        if(!cacheOverflow) {
            // results fit in memory. stream them out
            // TODO: in memory sort if necessary? or just always defer to gnu sort?
            final int rowsWritten = writeRowsToStream(resultsCache.iterator(), httpOutStream, csv, rowLimit, progress);
            final boolean exceedsRowLimit = rowsLoaded >= rowLimit && rows.hasNext();
            return new WriteResults(rowsWritten, null, resultsCache.iterator(), exceedsRowLimit);
        } else {    // have to work with the files on the hard drive to avoid OOM
            File unsortedFile = null;
            File sortedFile = null;
            try {
                unsortedFile = File.createTempFile(TEMP_FILE_PREFIX, null);
                // TODO: Use LimitedBufferedOutputStream or mark as skipped on limit
                final PrintWriter fileOutputStream = new PrintWriter(new OutputStreamWriter(
                        new BufferedOutputStream(new FileOutputStream(unsortedFile)), Charsets.UTF_8));
                final long started = System.currentTimeMillis();
                int rowsWritten = 0;
                // flush cache
                rowsWritten += writeRowsToStream(resultsCache.iterator(), fileOutputStream, csv, Integer.MAX_VALUE, false);
                //noinspection UnusedAssignment
                resultsCache = null;    // let it be GC'd
                // save the remaining rows to disk
                rowsWritten += writeRowsToStream(rows, fileOutputStream, csv, rowLimit - rowsWritten, false);
                fileOutputStream.close();
                log.trace("Stored on disk to " + unsortedFile.getPath() + " in " + (System.currentTimeMillis() - started) + "ms");

                if(requiresSorting) { // do on disk sort with gnu sort
                    sortedFile = sortFile(unsortedFile, groupingColumns, selectColumns);
                } else {
                    sortedFile = unsortedFile;
                }

                // send the results out to the client
                try {
                    copyStream(new FileInputStream(sortedFile), httpOutStream, rowLimit, progress);
                } finally {
                    if (sortedFile != unsortedFile) {
                        if (sortedFile.delete()) {
                            sortedFile = null;
                        } else {
                            log.warn("Failed to delete temporary file " + sortedFile.toString());
                        }
                    }
                }

                final boolean exceedsRowLimit = rowsWritten >= rowLimit && rows.hasNext();
                return new WriteResults(rowsWritten, unsortedFile, null, exceedsRowLimit);
            } catch (final Exception e) {
                if ((unsortedFile != null) && !unsortedFile.delete()) {
                    log.warn("Failed to delete temporary file " + unsortedFile.toString());
                }
                if ((sortedFile != null) && !sortedFile.delete()) {
                    log.warn("Failed to delete temporary file " + sortedFile.toString());
                }
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Sorts the given file by invoking gnu 'sort' command and returns a reference to the sorted copy.
     * Expects inputFile to have ".tmp" in the name
     */
    private File sortFile(File inputFile, int groupingColumns, int selectColumns) {
        File sortedFile = null;
        try {
            final long started;
            started = System.currentTimeMillis();
            sortedFile = new File(inputFile.getPath().replace(".tmp", ".sorted.tmp"));
            final List<String> sortCmd = Lists.newArrayList("sort", "-o", sortedFile.getPath(), "-t", "\t");

            // TODO: custom sorting orders
            for (int i = 1; i <= groupingColumns; i++) {
                sortCmd.add("-k" + i + "," + i);
            }
            for (int i = groupingColumns + 1; i <= groupingColumns + selectColumns; i++) {
                sortCmd.add("-k" + i + "," + i + "n");
            }
            sortCmd.add(inputFile.getPath());
            log.trace(IQLQuery.join(sortCmd, " "));

            final Process sortProc = Runtime.getRuntime().exec(sortCmd.toArray(new String[sortCmd.size()]), null);
            sortProc.waitFor();
            log.trace("Sorted to: " + sortedFile.getPath() + " in " + (System.currentTimeMillis() - started) + "ms");
            return sortedFile;
        } catch (final Exception e) {
            if ((sortedFile != null) && !sortedFile.delete()) {
                log.warn("Failed to delete temporary file " + sortedFile.toString());
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
    }

    private static String join(Collection items, String delimiter) {
        final StringBuilder sb = new StringBuilder(items.size() * 7);

        for (final Iterator it = items.iterator(); it.hasNext(); ) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    /**
     * Copies everything from input stream to the output stream, limiting to the requested number of lines if necessary.
     * Input stream is closed; output stream is flushed but not closed when done.
     */
    public static int copyStream(InputStream inputStream, PrintWriter outputStream, int lineLimit, boolean eventSource) {
        try {
            if(!eventSource && (lineLimit == Integer.MAX_VALUE || lineLimit <= 0)) {
                // no need to count rows so copy streams completely
                // we can't do this if we need the eventSource data
                CharStreams.copy(new InputStreamReader(inputStream, Charsets.UTF_8), outputStream);
                outputStream.flush();
                return 0;    // unknown how many rows were copied as we haven't counted
            }

            // have to count the lines as we copy to enforce the limit
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
            final BufferedWriter writer = new BufferedWriter(outputStream);

            if(eventSource) {
                final String EVENT_NAME = "resultstream";
                writer.write("event: " + EVENT_NAME);
                writer.newLine();
            }

            String line = reader.readLine();
            int linesCopied = 0;
            while(line != null) {
                if(eventSource) {
                    writer.write("data: ");
                }
                writer.write(line);
                writer.newLine();

                if(++linesCopied >= lineLimit) {
                    break;
                }

                line = reader.readLine();
            }
            Closeables2.closeQuietly(reader, log);
            writer.flush();
            return linesCopied;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            Closeables2.closeQuietly(inputStream, log);
        }
    }

    public static int writeRowsToStream(final Iterator<GroupStats> rows, PrintWriter out, final boolean csv, final int rowLimit, final boolean progress) {
        // TODO: how much precision do we want?
        final DecimalFormat format = new DecimalFormat("#.#######");
        final String tsvDelimiter = "\t";

        final CSVWriter csvWriter;
        final List<String> csvFields;
        if(csv) {
            csvWriter = new CSVWriter(out);
            csvFields = Lists.newArrayList();
        } else {
            csvWriter = null;
            csvFields = null;
        }
        int rowsProcessed = 0;

        if(progress) {
            out.println("event: resultstream");
        }

        while (rows.hasNext()) {
            final GroupStats entry = rows.next();
            if (entry == null) {
                continue;
            }
            if (progress) {
                out.print("data: ");
            }
            if(!csv) { // TSV
                GroupKey current = entry.getGroupKey();
                while (!current.isEmpty()) {
                    final Object group = current.head();
                    if(group instanceof String) {
                        final String groupString = (String) group;
                        // have to strip out reserved characters: tabs and new lines
                        for(int i = 0; i < groupString.length(); i++) {
                            final char groupChar = groupString.charAt(i);
                            if(groupChar != '\t' && groupChar != '\r' && groupChar != '\n') {
                                out.print(groupChar);
                            } else {
                                out.print('\ufffd'); // The replacement character
                            }
                        }
                    } else {
                        out.print(group);
                    }
                    current = current.tail();
                    if (!current.isEmpty()) {
                        out.print(tsvDelimiter);
                    }
                }
                for (double l : entry.getStats()) {
                    out.print(tsvDelimiter);
                    out.print(Double.isNaN(l) ? "NaN" : format.format(l));
                }
                out.println();
            } else {    // csv
                GroupKey current = entry.getGroupKey();
                while (!current.isEmpty()) {
                    csvFields.add(current.head().toString());
                    current = current.tail();
                }
                if (csvFields.isEmpty()) {
                    // Add empty column if no group by columns added
                    csvFields.add("");
                }
                for (double l : entry.getStats()) {
                    csvFields.add(format.format(l));
                }
                csvWriter.writeNext(csvFields.toArray(new String[csvFields.size()]));
                csvFields.clear();  // reused on next iteration
            }
            if(++rowsProcessed >= rowLimit) {
                break;   // reached the requested row limit
            }
        }
        out.flush();
        return rowsProcessed;
    }

    public List<Shard> getShards() {
        return shards;
    }

    public List<Interval> getTimeIntervalsMissingShards() {
        return timeIntervalsMissingShards;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public DateTime getStart(){ return start; }

    public DateTime getEnd() { return end; }

    public String getDataset() {
        return dataset;
    }

    public void addDeprecatedDatasetWarningIfNecessary(final List<String> warningList) {
        if (datasetMetadata.isDeprecatedOrDescriptionDeprecated()) {
            warningList.add("Dataset '" + dataset + "' is deprecated. Check the dataset description for alternative data sources.");
        }
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(strictCloser, log);
    }

    @Nullable
    public PerformanceStats closeAndGetPerformanceStats() {
        PerformanceStats performanceStats = null;
        if(session != null) {
            performanceStats = session.closeAndGetPerformanceStats();
        }
        close();
        return performanceStats;
    }

    public Set<String> getDatasetFields() {
        return this.datasetFields;
    }

    public Set<String> getFields() {
        return fields;
    }
}

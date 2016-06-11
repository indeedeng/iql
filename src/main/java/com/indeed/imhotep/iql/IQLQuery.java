/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.iql;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.indeed.util.core.TreeTimer;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import com.indeed.imhotep.web.ImhotepMetadataCache;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

import javax.annotation.Nonnull;
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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.indeed.imhotep.ez.Stats.Stat;

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
    private final ImhotepMetadataCache metadata;
    private final long docCountLimit;
    private final List<ShardIdWithVersion> shardVersionList;
    private final List<Interval> timeIntervalsMissingShards;
    private final ImhotepClient.SessionBuilder sessionBuilder;
    private final long shardsSelectionMillis;
    // session used for the current execution
    private EZImhotepSession session;

    public IQLQuery(ImhotepClient client, final List<Stat> stats, final String dataset, final DateTime start, final DateTime end,
                    final @Nonnull List<Condition> conditions, final @Nonnull List<Grouping> groupings, final int rowLimit,
                    final String username, ImhotepMetadataCache metadata) {
        this(client, stats, dataset, start, end, conditions, groupings, rowLimit, username, metadata, -1, -1, 0);
    }

    public IQLQuery(ImhotepClient client, final List<Stat> stats, final String dataset, final DateTime start, final DateTime end,
                    final @Nonnull List<Condition> conditions, final @Nonnull List<Grouping> groupings, final int rowLimit,
                    final String username, ImhotepMetadataCache metadata, final long imhotepLocalTempFileSizeLimit,
                    final long imhotepDaemonTempFileSizeLimit, final long docCountLimit) {
        this.stats = stats;
        this.dataset = dataset;
        this.start = start;
        this.end = end;
        this.conditions = conditions;
        this.groupings = groupings;
        this.rowLimit = rowLimit;
        this.metadata = metadata;
        this.docCountLimit = docCountLimit;

        long shardsSelectionStartTime = System.currentTimeMillis();
        sessionBuilder = client.sessionBuilder(dataset, start, end)
                .localTempFileSizeLimit(imhotepLocalTempFileSizeLimit)
                .daemonTempFileSizeLimit(imhotepDaemonTempFileSizeLimit).username(username);
        shardVersionList = sessionBuilder.getChosenShards();
        shardsSelectionMillis = System.currentTimeMillis() - shardsSelectionStartTime;

        timeIntervalsMissingShards = sessionBuilder.getTimeIntervalsMissingShards();
    }

    /**
     * Not thread safe due to session reference caching for close().
     */
    public ExecutionResult execute(boolean progress, OutputStream outputStream, boolean getTotals, SelectExecutionStats selectExecutionStats) throws ImhotepOutOfMemoryException {
        selectExecutionStats.setPhase("shardsSelectionMillis", shardsSelectionMillis);
        //if outputStream passed, update on progress
        final PrintWriter out = progress ? new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(outputStream), Charsets.UTF_8)) : null;

        final TreeTimer timer = new TreeTimer();
        timer.push("Imhotep session creation");
        final ImhotepSession imhotepSession = sessionBuilder.build();
        session = new EZImhotepSession(imhotepSession);

        final long numDocs = imhotepSession.getNumDocs();
        if (docCountLimit > 0 && numDocs > docCountLimit) {
            DecimalFormat df = new DecimalFormat("###,###");
            throw new LimitExceededException("The query on " + df.format(numDocs) +
                    " documents exceeds the limit of " + df.format(docCountLimit) + ". Please reduce the time range.");
        }
        selectExecutionStats.numDocs = numDocs;

        if(imhotepSession instanceof HasSessionId && selectExecutionStats != null) {
            selectExecutionStats.sessionId = ((HasSessionId) imhotepSession).getSessionId();
        }

        selectExecutionStats.setPhase("createSessionMillis", timer.pop());

        final long timeoutTS = System.currentTimeMillis() + executionTimeout.toStandardSeconds().getSeconds() * 1000;

        try {
            final int steps = conditions.size() + (groupings.size() == 0 ? 1 : groupings.size()) - 1;
            int count = 0;
            if(progress) {
                out.println(": Beginning IQL Query");
                out.println("event: totalsteps");
                out.print("data: " + steps + EVENT_SOURCE_END);
                out.print(": Starting time filter" + EVENT_SOURCE_END);
                out.flush();
            }
            timer.push("Time filter");
            timeFilter(session);
            selectExecutionStats.setPhase("timeFilterMillis", timer.pop());
            if(progress) {
                out.print(": Time filtering finished" + EVENT_SOURCE_END);
                out.flush();
            }

            long conditionFilterMillis = 0;
            for (int i = 0; i < conditions.size(); i++) {
                final Condition condition = conditions.get(i);
                checkTimeout(timeoutTS);
                timer.push("Filter " + (i + 1));
                condition.filter(session);
                conditionFilterMillis += timer.pop();
                count = updateProgress(progress, out, count);
            }
            selectExecutionStats.setPhase("conditionFilterMillis", conditionFilterMillis);

            long pushStatsMillis = 0;
            if (groupings.size() > 0) {
                List<StatReference> statRefs = null;
                double[] totals = new double[0];
                if(getTotals) {
                    timer.push("Pushing stats");
                    statRefs = pushStats(session);
                    pushStatsMillis += timer.pop();
                    timer.push("Getting totals");
                    totals = getStats(statRefs);
                    pushStatsMillis += timer.pop();
                }

                long regroupMillis = 0;
                Map<Integer, GroupKey> groupKeys = EZImhotepSession.newGroupKeys();
                // do Imhotep regroup on all except the last grouping
                for (int i = 0; i < groupings.size()-1; i++) {
                    checkTimeout(timeoutTS);
                    timer.push("Regroup " + (i + 1));
                    groupKeys = groupings.get(i).regroup(session, groupKeys);
                    selectExecutionStats.maxImhotepGroups = Math.max(selectExecutionStats.maxImhotepGroups, session.getNumGroups());
                    regroupMillis += timer.pop();
                    count = updateProgress(progress, out, count);
                }
                selectExecutionStats.setPhase("regroupMillis", regroupMillis);
                checkTimeout(timeoutTS);
                if(!getTotals) {
                    timer.push("Pushing stats");
                    statRefs = pushStats(session);
                    pushStatsMillis += timer.pop();
                }
                selectExecutionStats.setPhase("pushStatsMillis", pushStatsMillis);

                // do FTGS on the last grouping
                timer.push("FTGS");
                final Iterator<GroupStats> groupStatsIterator = groupings.get(groupings.size() - 1).getGroupStats(session, groupKeys, statRefs, timeoutTS);
                selectExecutionStats.maxImhotepGroups = Math.max(selectExecutionStats.maxImhotepGroups, session.getNumGroups());
                final long ftgsMillis = timer.pop();
                selectExecutionStats.setPhase("ftgsMillis", ftgsMillis);
                updateProgress(progress, out, count);
                return new ExecutionResult(groupStatsIterator, totals, timer.toString(), session.getTempFilesBytesWritten());
            } else {
                timer.push("Pushing stats");
                final List<StatReference> statRefs = pushStats(session);
                selectExecutionStats.setPhase("pushStatsMillis", timer.pop());
                timer.push("Getting stats");
                final double[] stats = getStats(statRefs);
                selectExecutionStats.setPhase("getStatsMillis", timer.pop());
                count = updateProgress(progress, out, count);
                final List<GroupStats> result = Lists.newArrayList();
                result.add(new GroupStats(GroupKey.<Comparable>empty(), stats));
                return new ExecutionResult(result.iterator(), stats, timer.toString(), session.getTempFilesBytesWritten());
            }
        } catch (Throwable t) {
            log.error("Error while executing the query", t);
            throw Throwables.propagate(t);
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

    private double[] getStats(List<StatReference> statRefs) {
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
        private final String timings;
        private final long imhotepTempFilesBytesWritten;

        public ExecutionResult(Iterator<GroupStats> rows, double[] totals, String timings, long imhotepTempFilesBytesWritten) {
            this.rows = rows;
            this.totals = totals;
            this.timings = timings;
            this.imhotepTempFilesBytesWritten = imhotepTempFilesBytesWritten;
        }

        public Iterator<GroupStats> getRows() {
            return rows;
        }

        public double[] getTotals() {
            return totals;
        }

        public String getTimings() {
            return timings;
        }

        public long getImhotepTempFilesBytesWritten() {
            return imhotepTempFilesBytesWritten;
        }
    }

    private void timeFilter(EZImhotepSession session) throws ImhotepOutOfMemoryException {
        final Pair<Long, Long> shardsMinMax = getShardsMinMax(shardVersionList);
        final long min = shardsMinMax.getFirst();
        final long max = shardsMinMax.getSecond();
        if (min < start.getMillis() || max > end.getMillis()) {
            new MetricCondition(EZImhotepSession.intField(getTimeField()),
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

    /**
     * Returns minimum and maximum milliseconds covered by the list of shards
     */
    private static Pair<Long, Long> getShardsMinMax(final List<ShardIdWithVersion> shardVersionList) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (ShardIdWithVersion shard : shardVersionList) {
            final ShardInfo.DateTimeRange interval = shard.getRange();
            if (interval.start.getMillis() < min) {
                min = interval.start.getMillis();
            }
            if (interval.end.getMillis() > max) {
                max = interval.end.getMillis();
            }
        }
        return Pair.of(min, max);
    }

    @Nonnull
    private String getTimeField() {
        return metadata.getDataset(dataset).getTimeFieldName();
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
        public final long timeTaken;
        public final boolean exceedsLimit;

        public WriteResults(int rowsWritten, File unsortedFile, Iterator<GroupStats> resultCacheIterator, long timeTaken, boolean exceedsLimit) {
            this.rowsWritten = rowsWritten;
            this.unsortedFile = unsortedFile;
            this.resultCacheIterator = resultCacheIterator;
            this.timeTaken = timeTaken;
            this.exceedsLimit = exceedsLimit;
        }

        public boolean didOverflowToDisk() {
            return unsortedFile != null;
        }
    }

    @Nonnull
    public WriteResults outputResults(final Iterator<GroupStats> rows, OutputStream httpOutStream, final boolean csv, final boolean progress, final int rowLimit, int groupingColumns, int selectColumns, boolean cacheDisabled) {
        final long timeStarted = System.currentTimeMillis();
        final boolean requiresSorting = requiresSorting();
        if(cacheDisabled && !requiresSorting) { // just stream the rows out. don't have to worry about keeping a copy at all
            final int rowsWritten = writeRowsToStream(rows, httpOutStream, csv, rowLimit, progress);
            return new WriteResults(rowsWritten, null, null, System.currentTimeMillis() - timeStarted, rows.hasNext());
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
            return new WriteResults(rowsWritten, null, resultsCache.iterator(), System.currentTimeMillis() - timeStarted, exceedsRowLimit);
        } else {    // have to work with the files on the hard drive to avoid OOM
            try {
                final File unsortedFile = File.createTempFile(TEMP_FILE_PREFIX, null);
                final FileOutputStream fileOutputStream = new FileOutputStream(unsortedFile);
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

                final File sortedFile;
                if(requiresSorting) { // do on disk sort with gnu sort
                    sortedFile = sortFile(unsortedFile, groupingColumns, selectColumns);
                } else {
                    sortedFile = unsortedFile;
                }

                // send the results out to the client
                copyStream(new FileInputStream(sortedFile), httpOutStream, rowLimit, progress);

                final boolean exceedsRowLimit = rowsWritten >= rowLimit && rows.hasNext();
                return new WriteResults(rowsWritten, unsortedFile, null, System.currentTimeMillis() - timeStarted, exceedsRowLimit);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Sorts the given file by invoking gnu 'sort' command and returns a reference to the sorted copy.
     * Expects inputFile to have ".tmp" in the name
     */
    private File sortFile(File inputFile, int groupingColumns, int selectColumns) {
        try {
            final long started;
            final File sortedFile;
            started = System.currentTimeMillis();
            sortedFile = new File(inputFile.getPath().replace(".tmp", ".sorted.tmp"));
            final List<String> sortCmd = Lists.newArrayList("sort", "-o", sortedFile.getPath(), "-t", "\t");

            // TODO: custom sorting orders
            for(int i = 1; i <= groupingColumns; i++) {
                sortCmd.add("-k" + i + "," + i);
            }
            for(int i = groupingColumns + 1; i <= groupingColumns + selectColumns; i++) {
                sortCmd.add("-k" + i + "," + i + "n");
            }
            sortCmd.add(inputFile.getPath());
            log.trace(IQLQuery.join(sortCmd, " "));

            final Process sortProc = Runtime.getRuntime().exec(sortCmd.toArray(new String[sortCmd.size()]), null);
            sortProc.waitFor();
            log.trace("Sorted to: " + sortedFile.getPath() + " in " + (System.currentTimeMillis() - started) + "ms");
            return sortedFile;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } catch (InterruptedException e) {
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
    public static int copyStream(InputStream inputStream, OutputStream outputStream, int lineLimit, boolean eventSource) {
        final String EVENT_NAME = "resultstream";
        try {
            if(!eventSource && (lineLimit == Integer.MAX_VALUE || lineLimit <= 0)) {
                // no need to count rows so copy streams completely
                // we can't do this if we need the eventSource data
                ByteStreams.copy(inputStream, outputStream);
                outputStream.flush();
                return 0;    // unknown how many rows were copied as we haven't counted
            }

            // have to count the lines as we copy to enforce the limit
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
            final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, Charsets.UTF_8));

            if(eventSource) {
                writer.write("event: " + EVENT_NAME);
                writer.newLine();
            }

            String line = reader.readLine();
            int linesCopied = 0;
            while(line != null) {
                if(eventSource) {
                    line = "data: " + line;
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

    public static int writeRowsToStream(final Iterator<GroupStats> rows, OutputStream os, final boolean csv, final int rowLimit, final boolean progress) {
        // TODO: how much precision do we want?
        final DecimalFormat format = new DecimalFormat("#.#######");
        final String tsvDelimiter = "\t";
        final PrintWriter out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(os), Charsets.UTF_8));

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
                GroupKey current = entry.groupKey;
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
                for (double l : entry.stats) {
                    out.print(tsvDelimiter);
                    out.print(Double.isNaN(l) ? "NaN" : format.format(l));
                }
                out.println();
            } else {    // csv
                GroupKey current = entry.groupKey;
                while (!current.isEmpty()) {
                    csvFields.add(current.head().toString());
                    current = current.tail();
                }
                for (double l : entry.stats) {
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

    public List<ShardIdWithVersion> getShardVersionList() {
        return shardVersionList;
    }

    public List<Interval> getTimeIntervalsMissingShards() {
        return timeIntervalsMissingShards;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public DateTime getStart(){ return start; }

    public DateTime getEnd() { return end; }

    @Override
    public void close() throws IOException {
        if(session != null) {
            Closeables2.closeQuietly(session, log);
        }
    }
}

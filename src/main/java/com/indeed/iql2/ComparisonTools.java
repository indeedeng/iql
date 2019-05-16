package com.indeed.iql2;

import com.indeed.imhotep.StrictCloser;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.language.SelectStatement;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.web.Limits;
import com.indeed.iql.web.QueryInfo;
import com.indeed.iql1.iql.IQLQuery;
import com.indeed.iql1.sql.IQLTranslator;
import com.indeed.iql1.sql.ast2.IQL1SelectStatement;
import com.indeed.iql1.sql.parser.SelectStatementParser;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.TimePeriods;
import com.indeed.iql2.language.optimizations.ConstantFolding;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.shardresolution.ImhotepClientShardResolver;
import com.indeed.iql2.server.web.servlets.query.CommandValidator;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Helper class for iql1 -> legacy mode transition period.
public class ComparisonTools {

    private ComparisonTools() {
    }

    // result of comparison of iql1 and legacy mode
    public enum Result {
        // Ok
        Equal(null),
        // Errors
        NotSupportedInLegacy("This query uses syntax that will be deprecated soon"),
        BothFail(null),
        ParsingErrorInLegacy("This query will fail to parse after iql1 backend replacement. See list of known incompatibilities for details."),
        ParsingErrorInIql1(null),
        // Known difference
        UnquotedTerm("Use quotes in around terms and in terms list. Unquoted terms will be deprecated soon"),
        CommentDiff("Query produce incorrect result because of incorrect comment processing. Delete all comments from query."),
        GroupByHour("'group by time(number)' syntax will be deprecated soon. Add explicit time units (hours)"),
        TermInParens("'field = (\"term\")' syntax will be deprecated soon. Rewrite query as 'field=\"term\"'"),
        // Unknown difference
        Unknown("This query will produce different result after iql1 backend replacement. See list of known incompatibilities for details.");

        @Nullable
        public final String message;

        Result(final String message) {
            this.message = message;
        }
    }

    // parse query as Iql1, convert to legacy mode, validate and return.
    // returns null if not a select query (describe, explain, ...)
    // throws exception if conversion fails or validation fails.
    @Nullable
    public static Query parseIQL1AndConvertToLegacyMode(
            final String query,
            final DatasetsMetadata datasetsMetadata,
            final ImhotepClient imhotepClient,
            final WallClock clock,
            final Limits limits) throws IQLQuery.Iql1ConvertException {

        final QueryInfo queryInfo = new QueryInfo("hostname", query,
                1, clock.currentTimeMillis(), null);
        final IQL1SelectStatement iql1SelectStatement =
                SelectStatementParser.parseSelectStatement(
                        query,
                        new DateTime(clock.currentTimeMillis(), DateTimeZone.forOffsetHours(-6)),
                        datasetsMetadata);

        final IQLQuery iql1Query = IQLTranslator.translate(
                iql1SelectStatement,
                imhotepClient,
                "compareIql1AndLegacy", // username. Will not be used since we don't create imhotep session here.
                datasetsMetadata,
                limits,
                queryInfo,
                new StrictCloser()
        );

        final Query convertedQuery = iql1Query.convertToIQL2Query(true);

        final Set<String> errors = new HashSet<>();
        final Set<String> warnings = new HashSet<>();
        CommandValidator.validate(convertedQuery, limits, datasetsMetadata, new ErrorCollector(errors, warnings));
        if (!errors.isEmpty()) {
            throw new IqlKnownException.ParseErrorException("Errors found when validating query: " + errors);
        }
        return convertedQuery;
    }

    public static Query parseLegacyMode(
            final String query,
            final DatasetsMetadata datasetsMetadata,
            final ImhotepClient imhotepClient,
            final WallClock clock,
            final Limits limits) {
        final Queries.ParseResult parseResult = Queries.parseQuery(
                query,
                true,
                datasetsMetadata,
                Collections.emptySet(),
                clock,
                new TracingTreeTimer(),
                new ImhotepClientShardResolver(imhotepClient)
        );
        final Set<String> errors = new HashSet<>();
        final Set<String> warnings = new HashSet<>();
        CommandValidator.validate(parseResult.query, limits, datasetsMetadata, new ErrorCollector(errors, warnings));
        if (!errors.isEmpty()) {
            throw new IqlKnownException.ParseErrorException("Errors found when validating query: " + errors);
        }
        return parseResult.query;
    }

    public static Query prepareQuery(final Query query) {
        Query result = ConstantFolding.apply(query);
        result = result.transform(
                makeCanonicalGroupBy(result.datasets.iterator().next()),
                Function.identity(),
                Function.identity(),
                Function.identity(),
                MAKE_DOC_FILTER_CANONICAL
        );
        return result;
    }

    // convert filter to some common format for further comparison
    private static final Function<DocFilter, DocFilter> MAKE_DOC_FILTER_CANONICAL = new Function<DocFilter, DocFilter>() {
        @Override
        public DocFilter apply(final DocFilter input) {
            if (input instanceof DocFilter.MetricGte) {
                // '>=' -> '>'
                final DocFilter.MetricGte metricGte = (DocFilter.MetricGte)input;
                if (metricGte.m2 instanceof DocMetric.Constant) {
                    return new DocFilter.MetricGt(metricGte.m1, new DocMetric.Constant(((DocMetric.Constant) metricGte.m2).value - 1));
                }
            } else if (input instanceof DocFilter.MetricLte) {
                // '<=' -> '<'
                final DocFilter.MetricLte metricLte = (DocFilter.MetricLte)input;
                if (metricLte.m2 instanceof DocMetric.Constant) {
                    return new DocFilter.MetricLt(metricLte.m1, new DocMetric.Constant(((DocMetric.Constant) metricLte.m2).value + 1));
                }
            } else if (input instanceof DocFilter.Sample) {
                final DocFilter.Sample sample = (DocFilter.Sample)input;
                // IQL1 and Legacy has different default salts, so replacing salt with default value.
                return new DocFilter.Sample(sample.field, sample.isIntField, sample.numerator, sample.denominator, "replacedSeed");
            }
            return input;
        }
    };

    private static Function<GroupBy, GroupBy> makeCanonicalGroupBy(final Dataset dataset) {
        return new Function<GroupBy, GroupBy>() {
            @Override
            public GroupBy apply(final GroupBy input) {
                if (input instanceof GroupBy.GroupByTimeBuckets) {
                    final GroupBy.GroupByTimeBuckets buckets = (GroupBy.GroupByTimeBuckets)input;
                    final long period = dataset.endExclusive.unwrap().getMillis() - dataset.startInclusive.unwrap().getMillis();
                    return new GroupBy.GroupByTime(period / buckets.numBuckets, buckets.field, buckets.format, buckets.isRelative);
                } else if (input instanceof GroupBy.GroupByInferredTime) {
                    final GroupBy.GroupByInferredTime inferredTime = (GroupBy.GroupByInferredTime)input;
                    final List<Dataset> datasets = Collections.singletonList(dataset);
                    final long periodMillis = TimePeriods.inferTimeBucketSize(
                            Dataset.getEarliestStart(datasets),
                            Dataset.getLatestEnd(datasets),
                            Dataset.getLongestRange(datasets),
                            inferredTime.isRelative);
                    return new GroupBy.GroupByTime(periodMillis, Optional.empty(), Optional.empty(), inferredTime.isRelative);
                }
                return input;
            }
        };
    }

    // .. group by ... time(number) .. means group by number hours.
    // not supported in legacy mode.
    private static final Pattern GROUP_BY_HOUR = Pattern.compile(".*group\\s+by.*(time|timebucket)\\s*\\(\\s*[0-9]+\\s*\\).*");
    // .. field = ("value") .. is not supported in legacy mode
    private static final Pattern TERM_IN_PARENS = Pattern.compile(".*(=|!=|=~|!=~)\\s*\\(.*");

    public static Result compareIql1AndLegacy(
            final SelectStatement selectStatement,
            final DatasetsMetadata datasetsMetadata,
            final ImhotepClient imhotepClient,
            final WallClock clock,
            final Limits limits) {
        Query iql1 = null;
        final String query = selectStatement.selectQuery;
        try {
            iql1 = parseIQL1AndConvertToLegacyMode(query, datasetsMetadata, imhotepClient, clock, limits);
        } catch (final IQLQuery.Iql1ConvertException e) {
            return Result.NotSupportedInLegacy;
        } catch (final Throwable ignored) {
        }

        Query legacy = null;
        try {
            legacy = parseLegacyMode(query, datasetsMetadata, imhotepClient, clock, limits);
        } catch (final Throwable ignored) {
        }

        if ((iql1 == null) && (legacy == null)) {
            // both fails, invalid query.
            return Result.BothFail;
        }

        if (iql1 == null) {
            // iql1 fails, legacy not
            return Result.ParsingErrorInIql1;
        }

        if (legacy == null) {
            // legacy fails, iql1 not
            // check known reasons
            if (hasUnquotedTerm(iql1, query)) {
                return Result.UnquotedTerm;
            }

            final Matcher groupByHour = GROUP_BY_HOUR.matcher(query);
            if (groupByHour.matches()) {
                return Result.GroupByHour;
            }

            final Matcher termInParens = TERM_IN_PARENS.matcher(query);
            if (termInParens.matches()) {
                return Result.TermInParens;
            }

            return Result.ParsingErrorInLegacy;
        }

        // both parse
        final Query canonicalIql1 = prepareQuery(iql1);
        final Query canonicalLegacy = prepareQuery(legacy);
        if (canonicalIql1.equals(canonicalLegacy)) {
            return Result.Equal;
        }

        // query parses in both versions but not match.
        // try to investigate why.
        if (isEmptyResultQuery(canonicalIql1) && isEmptyResultQuery(canonicalLegacy)) {
            return Result.Equal;
        }

        if (isIql1CommentError(iql1, legacy, query)) {
            return Result.CommentDiff;
        }

        return Result.Unknown;
    }

    public static Optional<String> checkCompatibility(
            final SelectStatement selectStatement,
            final DatasetsMetadata datasetsMetadata,
            final ImhotepClient imhotepClient,
            final WallClock clock,
            final Limits limits) {
        try {
            final Result comparisonResult = compareIql1AndLegacy(selectStatement, datasetsMetadata, imhotepClient, clock, limits);
            return Optional.ofNullable(comparisonResult.message);
        } catch (final Throwable t) {
            return Optional.of("Failed to compare original iql1 and legacy mode.");
        }
    }

    // does query produce empty result?
    private static boolean isEmptyResultQuery(final Query query) {
        if (query.filter.isPresent() && (query.filter.get() instanceof DocFilter.Never)) {
            return true;
        }

        for (final GroupByEntry entry : query.groupBys) {
            if (entry.filter.isPresent() && (entry.filter.get() instanceof AggregateFilter.Never)) {
                return true;
            }
            if (entry.groupBy instanceof GroupBy.GroupByFieldIn) {
                if (((GroupBy.GroupByFieldIn) entry.groupBy).terms.isEmpty()) {
                    return true;
                }
            }
        }

        return false;
    }

    // iql1 sometimes has more group by's selects or filters because it first split query
    // to parts and then parses it (comments are taken into account only on part parse phase)
    // for example "from dataset 5d today --group by time(1d)"
    // is treated as "from dataset 5d today group by time(1d)" in original iql1
    // and as "from dataset 5d today" in legacy mode.
    private static boolean isIql1CommentError(final Query iql1, final Query legacy, final String query) {
        if (!query.contains("--")) {
            return false;
        }

        if (iql1.rowLimit.isPresent() && !legacy.rowLimit.isPresent()) {
            return true;
        }
        return isIql1CommentError(asList(iql1.filter), asList(legacy.filter))
                || isIql1CommentError(iql1.groupBys, legacy.groupBys)
                || isIql1CommentError(iql1.selects, legacy.selects);
    }

    private static <T> boolean isIql1CommentError(final List<T> iql1, final List<T> legacy) {
        if (iql1.size() < legacy.size()) {
            return false; // ??? how could it be
        }
        if (iql1.size() == legacy.size()) {
            return false;
        }

        // iql.size() > legacy.size()
        for (int i = 0; i < legacy.size(); i++) {
            if (!iql1.get(i).equals(legacy.get(i))) {
                // we expect that iql1 has some extra components but other are equal
                return false;
            }
        }

        return true;
    }

    private static List<DocFilter> asList(final Optional<DocFilter> filter) {
        if (!filter.isPresent()) {
            return Collections.emptyList();
        }
        if (filter.get() instanceof DocFilter.And) {
            return ((DocFilter.Multiary) filter.get()).filters;
        } else {
            return Collections.singletonList(filter.get());
        }
    }

    private static boolean hasUnquotedTerm(final Query query, final String queryString) {
        final AtomicBoolean result = new AtomicBoolean(false);
        query.transform(
                new Function<GroupBy, GroupBy>() {
                    @Override
                    public GroupBy apply(final GroupBy groupBy) {
                        if (groupBy instanceof GroupBy.GroupByFieldIn) {
                            for (final Term term : ((GroupBy.GroupByFieldIn) groupBy).terms) {
                                if (isUnquotedTerm(term, queryString)) {
                                    result.set(true);
                                    return groupBy;
                                }
                            }
                        }
                        return groupBy;
                    }
                },
                Function.identity(),
                new Function<DocMetric, DocMetric>() {
                    @Override
                    public DocMetric apply(final DocMetric docMetric) {
                        if (docMetric instanceof DocMetric.HasString) {
                            if (isUnquotedTerm(((DocMetric.HasString) docMetric).term, queryString)) {
                                result.set(true);
                                return docMetric;
                            }
                        }
                        return docMetric;
                    }
                },
                Function.identity(),
                new Function<DocFilter, DocFilter>() {
                    @Override
                    public DocFilter apply(final DocFilter docFilter) {
                        if (docFilter instanceof DocFilter.FieldInTermsSet) {
                            for (final Term term : ((DocFilter.FieldInTermsSet) docFilter).terms) {
                                if (isUnquotedTerm(term, queryString)) {
                                    result.set(true);
                                    return docFilter;
                                }
                            }
                        } else if (docFilter instanceof DocFilter.FieldTermEqual) {
                            if (isUnquotedTerm(((DocFilter.FieldTermEqual) docFilter).term, queryString)) {
                                result.set(true);
                                return docFilter;
                            }
                        }
                        return docFilter;
                    }
                }
        );
        return result.get();
    }

    private static boolean isUnquotedTerm(final Term term, final String queryString) {
        return !term.isIntTerm() && isUnquotedTerm(term.asString(), queryString);
    }

    private static boolean isUnquotedTerm(final String term, final String queryString) {
        if (queryString.contains(term)
                && !queryString.contains('"' + term + '"')
                && !queryString.contains("'" + term + "'")) {
            // query has unquoted term.
            // check is this term is parsable as identifier
            if (Queries.tryRunParser(term, JQLParser::identifierTerminal) == null) {
                return true;
            }
        }
        return false;
    }
}

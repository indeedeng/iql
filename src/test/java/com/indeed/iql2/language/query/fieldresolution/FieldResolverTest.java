package com.indeed.iql2.language.query.fieldresolution;

import com.google.common.collect.ImmutableMap;
import com.indeed.common.datastruct.PersistentStack;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver.MetricResolverCallback;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.server.web.servlets.DimensionUtils;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.function.Consumer;

import static com.indeed.iql2.language.query.fieldresolution.FieldResolver.FAILED_TO_RESOLVE_DATASET;
import static com.indeed.iql2.language.query.fieldresolution.FieldResolver.FAILED_TO_RESOLVE_FIELD;
import static com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver.PLAIN_DOC_METRIC_CALLBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author jwolfe
 */
public class FieldResolverTest {
    private static final Consumer<String> WARN = s -> System.out.println("PARSE WARNING: " + s);
    private static final WallClock CLOCK = new StoppedClock(new DateTime(2015, 2, 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());
    private static final FieldResolver FIELD_RESOLVER = FieldResolverTest.fromQuery("from organic 2d 1d");
    private static final Query.Context CONTEXT = new Query.Context(
            Collections.emptyList(),
            AllData.DATASET.getDatasetsMetadata(),
            null,
            WARN,
            CLOCK,
            new TracingTreeTimer(),
            FIELD_RESOLVER.universalScope(),
            new NullShardResolver(),
            PersistentStack.empty(),
            DateTimeZone.forOffsetHours(-6));

    public static FieldResolver fromQuery(final String query) {
        final boolean useLegacy = false;
        final JQLParser.QueryContext parseResult = Queries.parseQueryContext(query, useLegacy);
        final FieldResolver resolver = FieldResolver.build(parseResult, parseResult.fromContents(),
                AllData.DATASET.getDatasetsMetadata(), useLegacy);
        resolver.setErrorMode(FieldResolver.ErrorMode.IMMEDIATE);
        return resolver;
    }

    private static JQLParser.IdentifierContext parseIdentifier(final String input) {
        return Queries.runParser(input, JQLParser::identifierTerminal).identifier();
    }

    private static JQLParser.SinglyScopedFieldContext parseSinglyScopedField(final String input) {
        return Queries.runParser(input, JQLParser::singlyScopedFieldTerminal).singlyScopedField();
    }

    @Test
    public void testSimpleCase() {
        final FieldResolver fieldResolver = fromQuery("from organic 2d 1d");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        assertEquals(FieldSet.of("organic", "oji", true), scopedResolver.resolve(parseIdentifier("oji")));
        assertEquals(FieldSet.of("organic", "tk", false), scopedResolver.resolve(parseIdentifier("tk")));
        assertEquals(FieldSet.of("organic", "ojc", true), scopedResolver.resolve(parseIdentifier("ojc")));
    }

    @Test
    public void testCaseInsensitiveDataset() {
        final FieldResolver fieldResolver = fromQuery("from OrGaNiC 2d 1d");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        assertEquals(FieldSet.of("organic", "oji", true), scopedResolver.resolve(parseIdentifier("oji")));
        assertEquals(FieldSet.of("organic", "tk", false), scopedResolver.resolve(parseIdentifier("tk")));
        assertEquals(FieldSet.of("organic", "ojc", true), scopedResolver.resolve(parseIdentifier("ojc")));
    }

    @Test
    public void testCaseInsensitiveField() {
        final FieldResolver fieldResolver = fromQuery("from organic 2d 1d");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        assertEquals(FieldSet.of("organic", "oji", true), scopedResolver.resolve(parseIdentifier("oJi")));
        assertEquals(FieldSet.of("organic", "tk", false), scopedResolver.resolve(parseIdentifier("TK")));
        assertEquals(FieldSet.of("organic", "ojc", true), scopedResolver.resolve(parseIdentifier("OjC")));
    }

    @Test
    public void testNonExistentDataset() {
        final FieldResolver fieldResolver = fromQuery("from organic 2d 1d");
        fieldResolver.setErrorMode(FieldResolver.ErrorMode.DEFERRED);
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        assertEquals(FieldSet.of(FAILED_TO_RESOLVE_DATASET, FAILED_TO_RESOLVE_FIELD, false), scopedResolver.resolve(parseSinglyScopedField("MyFakeDataset.oji")));
        final IqlKnownException error = fieldResolver.errors();
        assertNotNull(error);
        assertTrue(error instanceof IqlKnownException.UnknownDatasetException);
        final Throwable[] suppressed = error.getSuppressed();
        assertEquals(1, suppressed.length);
        assertTrue(suppressed[0] instanceof IqlKnownException.UnknownFieldException);
    }

    @Test
    public void testNonExistentField() {
        final FieldResolver fieldResolver = fromQuery("from organic 2d 1d");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        try {
            scopedResolver.resolve(parseIdentifier("MyFakeField"));
            Assert.fail("Expected UnknownFieldException");
        } catch (final IqlKnownException.UnknownFieldException ignored) {
        }
    }

    @Test
    public void testAlias() {
        final FieldResolver fieldResolver = fromQuery("from organic 2d 1d aliasing (oji as imps)");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        assertEquals(FieldSet.of("organic", "oji", true), scopedResolver.resolve(parseIdentifier("imps")));
        assertEquals(FieldSet.of("organic", "oji", true), scopedResolver.resolve(parseIdentifier("iMPs")));
    }

    @Test
    public void testNamedMetric() {
        final FieldResolver fieldResolver = fromQuery("from organic 2d 1d select oji+3 as oji3, oji3");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        assertEquals(new AggregateMetric.NeedsSubstitution("oji3"), scopedResolver.resolveMetricAlias(parseIdentifier("oji3")));
        assertEquals(new AggregateMetric.NeedsSubstitution("oji3"), scopedResolver.resolveMetricAlias(parseIdentifier("oJI3")));

        try {
            scopedResolver.resolve(parseIdentifier("oji3"));
            Assert.fail("Expected UnknownFieldException");
        } catch (final IqlKnownException.UnknownFieldException ignored) {
        }
    }

    @Test
    public void testMultipleDatasets() {
        final FieldResolver fieldResolver = fromQuery("from dataset1 2d 1d, dataset2 aliasing (intField3 as intField2)");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
        assertEquals(FieldSet.of("dataset1", "intField1", "dataset2", "intField1", true), scopedResolver.resolve(parseIdentifier("intField1")));
        assertEquals(FieldSet.of("dataset1", "intField2", "dataset2", "intField3", true), scopedResolver.resolve(parseIdentifier("intField2")));
        assertEquals(FieldSet.of("dataset2", "intField3", true), scopedResolver.resolve(parseSinglyScopedField("dataset2.intField2")));
        final ScopedFieldResolver smallerScopedResolver = scopedResolver.forScope(Collections.singleton("dataset2"));
        assertEquals(FieldSet.of("dataset2", "intField3", true), smallerScopedResolver.resolve(parseIdentifier("intField3")));
        assertEquals(FieldSet.of("dataset2", "intField3", true), smallerScopedResolver.resolve(parseIdentifier("intField2")));

        try {
            scopedResolver.resolve(parseIdentifier("intField3"));
            Assert.fail("Expected UnknownFieldException");
        } catch (final IqlKnownException.UnknownFieldException ignored) {
        }
    }

    private static final ImhotepMetadataCache DIMENSIONS_METADATA = new ImhotepMetadataCache(new DimensionUtils.ImsClient(), AllData.DATASET.getNormalClient(), "", new FieldFrequencyCache(null));
    static {
        DIMENSIONS_METADATA.updateDatasets();
    }

    // same as fromQuery, but uses the dimensions data
    private static FieldResolver fromQueryDimensions(final String query) {
        final boolean useLegacy = false;
        final JQLParser.QueryContext parseResult = Queries.parseQueryContext(query, useLegacy);
        final DatasetsMetadata datasetsMetadata = DIMENSIONS_METADATA.get();
        final FieldResolver resolver = FieldResolver.build(parseResult, parseResult.fromContents(),
                datasetsMetadata, useLegacy);
        resolver.setErrorMode(FieldResolver.ErrorMode.IMMEDIATE);
        return resolver;
    }

    @Test
    public void testDimensionsDocMetric() {
        final DocMetric failure = new DocMetric.Field(FieldSet.of("Failure", "Failure", true));

        final FieldResolver fieldResolver = fromQueryDimensions("from dimension 2d 1d");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();

        final DocMetric.Field i1 = new DocMetric.Field(FieldSet.of("DIMension", "i1", true));
        assertEquals(i1, scopedResolver.resolveDocMetric(parseIdentifier("i1"), PLAIN_DOC_METRIC_CALLBACK, CONTEXT));

        final DocMetric.Field i2 = new DocMetric.Field(FieldSet.of("DIMension", "i2", true));
        assertEquals(DocMetric.Add.create(i1, i2), scopedResolver.resolveDocMetric(parseIdentifier("plus"), PLAIN_DOC_METRIC_CALLBACK, CONTEXT));

        // Ensure it's not wrapped in qualified because {DIMension}={DIMension}
        assertEquals(DocMetric.Add.create(i1, i2), scopedResolver.resolveDocMetric(parseSinglyScopedField("dimension.plus"), PLAIN_DOC_METRIC_CALLBACK, CONTEXT));
    }

    @Test
    public void testDimensionsDocFilter() {
        final DocFilter.Never failure = new DocFilter.Never();
        final DocFilter.Always success = new DocFilter.Always();

        final FieldResolver fieldResolver = fromQueryDimensions("from dimension 2d 1d");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();

        assertEquals(success, scopedResolver.resolveDocFilter(parseIdentifier("i1"), new MetricResolverCallback<DocFilter>() {
            @Override
            public DocFilter plainFields(final FieldSet fieldSet) {
                Assert.assertEquals(FieldSet.of("DIMension", "i1", true), fieldSet);
                return success;
            }

            @Override
            @Nullable
            public DocFilter metric(final DocMetric metric) {
                Assert.fail("Should not find metric");
                return null;
            }
        }, CONTEXT));

        final DocMetric.Field i1 = new DocMetric.Field(FieldSet.of("DIMension", "i1", true));
        final DocMetric.Field i2 = new DocMetric.Field(FieldSet.of("DIMension", "i2", true));
        assertEquals(success, scopedResolver.resolveDocFilter(parseIdentifier("plus"), new MetricResolverCallback<DocFilter>() {
            @Override
            public DocFilter plainFields(final FieldSet fieldSet) {
                Assert.fail("Should find metric");
                return failure;
            }

            @Override
            public DocFilter metric(final DocMetric metric) {
                Assert.assertEquals(DocMetric.Add.create(i1, i2), metric);
                return success;
            }
        }, CONTEXT));

        // Ensure it's not wrapped in qualified because {DIMension}={DIMension}
        assertEquals(success, scopedResolver.resolveDocFilter(parseSinglyScopedField("dimension.plus"), new MetricResolverCallback<DocFilter>() {
            @Override
            public DocFilter plainFields(final FieldSet fieldSet) {
                Assert.fail("Should find metric");
                return failure;
            }

            @Override
            public DocFilter metric(final DocMetric metric) {
                Assert.assertEquals(DocMetric.Add.create(i1, i2), metric);
                return success;
            }
        }, CONTEXT));
    }

    @Test
    public void testDimensionsAggregateMetric() {
        final FieldResolver fieldResolver = fromQueryDimensions("from dimension 2d 1d");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();

        final DocMetric.Field i1 = new DocMetric.Field(FieldSet.of("DIMension", "i1", true));
        final DocMetric.Field i2 = new DocMetric.Field(FieldSet.of("DIMension", "i2", true));
        assertEquals(new AggregateMetric.DocStats(i1), scopedResolver.resolveAggregateMetric(parseIdentifier("i1"), CONTEXT));
        assertEquals(new AggregateMetric.DocStats(DocMetric.Add.create(i1, i2)), scopedResolver.resolveAggregateMetric(parseIdentifier("plus"), CONTEXT));
        assertEquals(new AggregateMetric.DocStats(DocMetric.Add.create(i1, i2)), scopedResolver.resolveAggregateMetric(parseSinglyScopedField("dimension.plus"), CONTEXT));
    }

    @Test
    public void testMultiDatasetDimensions() {
        final FieldResolver fieldResolver = fromQueryDimensions("from dimension 2d 1d, dimension2");
        final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();

        assertEquals(new DocMetric.Field(FieldSet.of("DIMension", "i1", "dimension2", "i1", true)), scopedResolver.resolveDocMetric(parseIdentifier("i1"), PLAIN_DOC_METRIC_CALLBACK, CONTEXT));

        assertEquals(new DocMetric.Field(FieldSet.of("DIMension", "i2", "dimension2", "i1", true)), scopedResolver.resolveDocMetric(parseIdentifier("i2"), PLAIN_DOC_METRIC_CALLBACK, CONTEXT));

        final DocMetric.Qualified qualifiedExpected = new DocMetric.Qualified("DIMension", new DocMetric.Field(FieldSet.of("DIMension", "i1", true)));
        assertEquals(qualifiedExpected, scopedResolver.resolveDocMetric(parseSinglyScopedField("dimension.aliasi1"), PLAIN_DOC_METRIC_CALLBACK, CONTEXT));

        final DocMetric.PerDatasetDocMetric calcExpected = new DocMetric.PerDatasetDocMetric(
                ImmutableMap.of(
                        "DIMension",
                        new DocMetric.Multiply( // calc = (i1 + i2) * 10
                                DocMetric.Add.create(
                                        new DocMetric.Field(FieldSet.of("DIMension", "i1", true)),
                                        new DocMetric.Field(FieldSet.of("DIMension", "i2", true))
                                ),
                                new DocMetric.Constant(10)
                        ),
                        "dimension2",
                        new DocMetric.Multiply( // calc = (i1 + i2) * 10
                                DocMetric.Add.create(
                                        new DocMetric.Field(FieldSet.of("dimension2", "i1", true)),
                                        new DocMetric.Field(FieldSet.of("dimension2", "i1", true)) // i2 is aliased to i1
                                ),
                                new DocMetric.Constant(10)
                        )
                )
        );
        assertEquals(calcExpected, scopedResolver.resolveDocMetric(parseIdentifier("calc"), PLAIN_DOC_METRIC_CALLBACK, CONTEXT));
    }

    @Test
    public void testIncorrectQualifiedDataset() {
        final FieldResolver fieldResolver;
        try {
            fieldResolver = fromQuery("from organic 2d 1d");
            fieldResolver.setErrorMode(FieldResolver.ErrorMode.DEFERRED);
            final ScopedFieldResolver scopedResolver = fieldResolver.universalScope();
            assertNotNull(scopedResolver.resolveAggregateMetric(parseSinglyScopedField("foo.field"), CONTEXT));
        } catch (final Exception e) {
            Assert.fail("Not supposed to throw an exception yet");
            // required for compiler to understand that fieldResolver is always initialized in the next line
            throw new RuntimeException();
        }
        try {
            fieldResolver.setErrorMode(FieldResolver.ErrorMode.IMMEDIATE);
            Assert.fail();
        } catch (final IqlKnownException.UnknownDatasetException e) {
            assertEquals("Dataset not found or not included in query: \"foo\"", e.getMessage());
        }
    }
}
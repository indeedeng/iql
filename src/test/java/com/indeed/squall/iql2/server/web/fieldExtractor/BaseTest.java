package com.indeed.squall.iql2.server.web.fieldExtractor;

import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.util.FieldExtractor;
import com.indeed.squall.iql2.language.util.FieldExtractor.DatasetField;
import com.indeed.squall.iql2.server.web.metadata.MetadataCache;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;

import java.util.Set;

/**
 * @author jessec@indeed.com (Jesse Chen)
 */

public class BaseTest {

	protected static final DatasetField ORGANIC_TK = new DatasetField("TK", "ORGANIC", true);

	protected static final DatasetField ORGANIC_OJI = new DatasetField("OJI", "ORGANIC", true);

	protected static final DatasetField ORGANIC_OJC = new DatasetField("OJC", "ORGANIC", true);

	protected static final DatasetField ORGANIC_UNIXTIME = new DatasetField("UNIXTIME", "ORGANIC", true);

	protected static final DatasetField JOBSEARCH_CTKRCVD = new DatasetField("CTKRCVD", "JOBSEARCH", true);

	private static final WallClock wallClock = new StoppedClock(new DateTime(2015, 1, 2, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());

	public void verify(final Set<DatasetField> expected, final String query) {
		final Queries.ParseResult parseResult = Queries.parseQuery(
				query,
				false,
				new MetadataCache(null, null).get(),
				new com.indeed.squall.iql2.language.compat.Consumer<String>() {
					@Override
					public void accept(String s) {}
				},
				wallClock);
			Assert.assertEquals(expected, FieldExtractor.getDatasetFields(parseResult.query));
	}
}

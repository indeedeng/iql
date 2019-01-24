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

package com.indeed.iql2.server.web.fieldExtractor;

import java.util.Collections;
import java.util.function.Consumer;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.iql2.language.util.FieldExtractor;
import com.indeed.iql2.language.util.FieldExtractor.DatasetField;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;

import java.util.Set;

/**
 * @author jessec@indeed.com (Jesse Chen)
 */

public class BaseTest {

	protected static final DatasetField ORGANIC_TK = new DatasetField("tk", "organic", true);

	protected static final DatasetField ORGANIC_OJI = new DatasetField("oji", "organic", true);

	protected static final DatasetField ORGANIC_OJC = new DatasetField("ojc", "organic", true);

	protected static final DatasetField ORGANIC_UNIXTIME = new DatasetField("unixtime", "organic", true);

	protected static final DatasetField JOBSEARCH_UNIXTIME = new DatasetField("unixtime", "jobsearch", true);

	protected static final DatasetField JOBSEARCH_CTKRCVD = new DatasetField("ctkrcvd", "jobsearch", true);

	private static final WallClock wallClock = new StoppedClock(new DateTime(2015, 1, 2, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());
	public static final TracingTreeTimer TIMER = new TracingTreeTimer();
	public static final ShardResolver SHARD_RESOLVER = new NullShardResolver();

	public void verify(final Set<DatasetField> expected, final String query) {
		final Queries.ParseResult parseResult = Queries.parseQuery(
				query,
				false,
				AllData.DATASET.getDatasetsMetadata(),
                Collections.emptySet(),
                new Consumer<String>() {
					@Override
					public void accept(String s) {}
				},
				wallClock,
				TIMER,
				SHARD_RESOLVER
		);
			Assert.assertEquals(expected, FieldExtractor.getDatasetFields(parseResult.query));
	}
}

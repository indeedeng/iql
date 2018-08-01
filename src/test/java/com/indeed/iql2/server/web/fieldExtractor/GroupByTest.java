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

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

/**
 * @author jessec@indeed.com (Jesse Chen)
 */

public class GroupByTest extends BaseTest {

	@Test
	public void testGroupByField() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today group by tk");
	}

	@Test
	public void testGroupByPredicate() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today group by tk = 'a'");
	}

	@Test
	public void testGroupByMetric() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today group by bucket(oji, 0, 11, 1)");
	}

	@Test
	public void testGroupByTime() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today group by time(1d, 'yyyy-MM-dd', unixtime)");
	}

	@Test
	public void testGroupByTimeBucket() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today group by time(1b, 'yyyy-MM-dd', unixtime)");
	}

	@Test
	public void testGroupByMonth() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today group by time(1M, 'yyyy-MM-dd', unixtime)");
	}

	@Test
	public void testGroupByFieldIn() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today group by oji in (1, 2, 3)");
	}

	@Test
	public void testGroupByDayOfWeek() {
		verify(ImmutableSet.of(), "from organic yesterday today group by dayofweek()");
	}

	@Test
	public void testGroupBySessionName() {
		//TODO
	}

	@Test
	public void testGroupByQuantiles() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today group by quantiles(oji, 50)");
	}

	@Test
	public void testGroupByRandom() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today group by random(unixtime, 5)");
	}

	@Test
	public void testGroupByRandomMetric() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today group by random(hasint(oji, 1), 5)");
	}
}

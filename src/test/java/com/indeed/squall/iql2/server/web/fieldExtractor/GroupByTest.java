package com.indeed.squall.iql2.server.web.fieldExtractor;

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

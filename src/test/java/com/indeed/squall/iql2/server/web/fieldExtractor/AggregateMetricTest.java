package com.indeed.squall.iql2.server.web.fieldExtractor;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

/**
 * @author jessec@indeed.com(Jesse Chen)
 */

public class AggregateMetricTest extends BaseTest{

	@Test
	public void testAdd() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select oji + ojc");
	}

	@Test
	public void testLog() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select log(oji)");
	}

	@Test
	public void testNegate() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select -oji");
	}

	@Test
	public void testAbs() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select abs(oji)");
	}

	@Test
	public void testSubstract() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select oji - ojc");
	}

	@Test
	public void testMultiply() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select oji * ojc");
	}

	@Test
	public void testDevide() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select oji / ojc");
	}

	@Test
	public void testModulus() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select oji % ojc");
	}

	@Test
	public void testPower() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select oji ^ ojc");
	}

	@Test
	public void testParent() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select parent(oji)");
	}

	@Test
	public void testLag() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select lag(5, oji)");
	}

	@Test
	public void testIterateLag() {
		//TODO
	}

	@Test
	public void testWindow() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select window(5, oji)");
	}

	@Test
	public void testQualified() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select organic.oji");
	}

	@Test
	public void testStatsPushes() {
		//TODO
	}

	@Test
	public void testConstant() {
		verify(ImmutableSet.of(), "from organic yesterday today select 5");
	}

	@Test
	public void testPercentile() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select percentile(oji, 95)");
	}

	@Test
	public void testRunning() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select running(oji)");
	}

	@Test
	public void testDistinct() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select distinct(oji)");
	}

	@Test
	public void testNamed() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select distinct(oji) as doji");
	}

	@Test
	public void testGroupStatsLookup() {
		//TODO
	}

	@Test
	public void testGroupStatsMultiLookup() {
		//TODO
	}

	@Test
	public void testSumAcross() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select sum_over(oji, ojc)");
	}

	@Test
	public void testIfThenElse() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select if oji > 0 then ojc else oji");
	}

	@Test
	public void testFieldMinAndFieldMax() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select field_min(oji), field_max(ojc)");
	}


	@Test
	public void testMax() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select max(oji, ojc)");
	}

	@Test
	public void testMin() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select min(oji, ojc)");
	}

	@Test
	public void testBootstrap() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select bootstrap(oji, ojc, 5, 'a')");
	}

	@Test
	public void testDivideByCount() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select variance(oji/count())");
	}
}

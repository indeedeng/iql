package com.indeed.squall.iql2.server.web.fieldExtractor;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

/**
 * @author jessec@indeed.com (Jesse Chen)
 */

public class MultidatasetTest extends BaseTest{

	@Test
	public void testNoScope() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME, JOBSEARCH_UNIXTIME), "from organic yesterday today, jobsearch where unixtime > 0");
	}

	@Test
	public void testAliasNoScope() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME, JOBSEARCH_UNIXTIME), "from organic yesterday today as org, jobsearch as js where unixtime > 0");
	}

	@Test
	public void testDoubleAliases() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today as control, organic as test select control.unixtime, test.unixtime");
	}

	@Test
	public void testAliasWithScope() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today as org, jobsearch as js where org.unixtime > 0");
	}

	@Test
	public void testFieldAlias() {
		verify(ImmutableSet.of(JOBSEARCH_UNIXTIME, ORGANIC_TK), "from organic yesterday today aliasing (tk as x), jobsearch aliasing (unixtime as x) where x > 0");
	}

	@Test
	public void testAliasAndFieldAlias() {
		verify(ImmutableSet.of(JOBSEARCH_UNIXTIME), "from organic yesterday today as org aliasing (tk as x), jobsearch as js aliasing (unixtime as x) where js.x > 0");
	}

	@Test
	public void testFieldInQuery() {
		verify(ImmutableSet.of(JOBSEARCH_CTKRCVD, ORGANIC_TK, ORGANIC_UNIXTIME), "from organic yesterday today where tk in (from jobsearch yesterday today group by ctkrcvd) select unixtime");
	}

}

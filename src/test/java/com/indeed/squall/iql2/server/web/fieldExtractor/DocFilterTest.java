package com.indeed.squall.iql2.server.web.fieldExtractor;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

/**
 * @author jessec@indeed.com (Jesse Chen)
 */

public class DocFilterTest extends BaseTest {

	@Test
	public void testFieldIs() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today where tk=\"a\" ");
	}

	@Test
	public void testFieldIsnt() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today where tk!=\"a\" ");
	}

	@Test
	public void testMetricEqual() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today where ABS(ojc) = ABS(oji) ");
	}

	@Test
	public void testFieldInQuery() {
		verify(ImmutableSet.of(ORGANIC_TK, JOBSEARCH_CTKRCVD), "from organic yesterday today where tk not in (from jobsearch yesterday today group by ctkrcvd)");
	}

	@Test
	public void testBetween() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today where between(unixtime, 0, 1000)");
	}

	@Test
	public void testMetricNotEqual() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today where EXP(ojc) != EXP(oji)");
	}

	@Test
	public void testMetricGt() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today where EXP(ojc) > EXP(oji)");
	}

	@Test
	public void testMetricGte() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today where EXP(ojc) >= EXP(oji)");
	}

	@Test
	public void testMetricLt() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today where EXP(ojc) < EXP(oji)");
	}

	@Test
	public void testMetricLte() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today where EXP(ojc) <= EXP(oji)");
	}

	@Test
	public void testAnd() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME, ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where EXP(ojc) <= EXP(oji) and between(unixtime, 0, 1000)");
	}

	@Test
	public void testOr() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME, ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where EXP(ojc) <= EXP(oji) or between(unixtime, 0, 1000)");
	}

	@Test
	public void testNot() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today where not(tk=\"a\") ");
	}

	@Test
	public void testRegex() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today where tk =~ \"a.*\" ");
	}

	@Test
	public void testNotRegex() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today where tk !=~ \"a.*\" ");
	}

	@Test
	public void testQualified() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today where organic.tk = \"a\"");
	}

	@Test
	public void testLucene() {
		verify(ImmutableSet.of(), "from organic yesterday today where lucene(\"oji:3\")");
	}

	@Test
	public void testSample() {
		verify(ImmutableSet.of(ORGANIC_UNIXTIME), "from organic yesterday today where sample(unixtime, 1)");
	}

	@Test
	public void testAlways() {
		verify(ImmutableSet.of(), "from organic yesterday today where true");
	}

	@Test
	public void testNever() {
		verify(ImmutableSet.of(), "from organic yesterday today where false");
	}

	@Test
	public void testStringFieldIn() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today where tk in ('a', 'b')");
	}

	@Test
	public void testIntFieldIn() {
		verify(ImmutableSet.of(ORGANIC_OJC), "from organic yesterday today where ojc in (1, 2)");
	}

	@Test
	public void explainFieldIn() {
		//TODO
	}

	@Test
	public void testFieldEqual() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today where ojc = oji");
	}

}

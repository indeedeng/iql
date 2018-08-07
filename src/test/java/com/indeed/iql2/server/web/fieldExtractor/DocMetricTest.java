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

public class DocMetricTest extends BaseTest {

	@Test
	public void testLog() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select log(oji)");
	}

	@Test
	public void testPushableDocMetric() {
		//TODO
	}

	@Test
	public void testPerDatasetDocMetric() {
		//TODO
	}

	@Test
	public void testCount() {
		verify(ImmutableSet.of(), "from organic yesterday today select count()");
	}

	@Test
	public void testDocId() {
		verify(ImmutableSet.of(), "from organic yesterday today where docid() > 0");
	}

	@Test
	public void testDocField() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select tk");
	}

	@Test
	public void testExponentiate() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today where EXP(oji) > 0");
	}

	@Test
	public void testNegate() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today where -EXP(oji) < 0");
	}

	@Test
	public void testAbs() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today where ABS(oji) < 0");
	}

	@Test
	public void testSignum() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today where SIGNUM(oji) < 0");
	}

	@Test
	public void testAdd() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where oji + ojc = 5");
	}

	@Test
	public void testSubtract() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where oji - ojc = 5");
	}

	@Test
	public void testMultiply() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where oji * ojc = 5");
	}

	@Test
	public void testDivide() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where oji / ojc = 5");
	}

	@Test
	public void testModulus() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where oji % ojc = 5");
	}

	@Test
	public void testMin() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where min(oji, ojc) = 5");
	}

	@Test
	public void testMax() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where max(oji, ojc) = 5");
	}

	@Test
	public void testMetricEqualAndMetricNotEqual() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where (abs(oji) = abs(ojc)) = (signum(oji) != signum(ojc))");
	}

	@Test
	public void testMetricLtAndMetricGt() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where (abs(oji) < abs(ojc)) = (signum(oji) > signum(ojc))");
	}

	@Test
	public void testMetricLteAndMetricGte() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today where (abs(oji) <= abs(ojc)) = (signum(oji) >= signum(ojc))");
	}

	@Test
	public void testExtract() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select extract(tk, '.*')");
	}

	@Test
	public void testRegex() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select [tk =~ '.*']");
	}

	@Test
	public void testFloatscale() {
		verify(ImmutableSet.of(ORGANIC_OJC), "from organic yesterday today select [floatscale(ojc, 1, 2)]");
	}

	@Test
	public void testConstant() {
		verify(ImmutableSet.of(), "from organic yesterday today select [5]");
	}

	@Test
	public void testHasIntField() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic(hasintfield(oji)>0) yesterday today");
	}

	@Test
	public void testHasStringField() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic(hasstrfield(tk)>0) yesterday today");
	}

	@Test
	public void testIntTermCount() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select inttermcount(oji)");
	}

	@Test
	public void testStringTermCount() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select strtermcount(tk)");
	}

	@Test
	public void testHasInt() {
		verify(ImmutableSet.of(ORGANIC_OJI), "from organic yesterday today select [hasint(oji, 5)]");
	}

	@Test
	public void testHasString() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select [hasstr(tk, 'a')]");
	}

	@Test
	public void testIfThenElse() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select [if(tk = 'a') then 1 else 0]");
	}

	@Test
	public void testQualified() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select [organic.tk]");
	}

	@Test
	public void testFieldEqual() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC), "from organic yesterday today select oji = ojc");
	}

	@Test
	public void testFieldLen() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today select len(tk)");
	}
}

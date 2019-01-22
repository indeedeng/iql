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

public class AggregateFilterTest extends BaseTest {

	@Test
	public void testTermIs() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today group by tk having term() = 'a'");
	}

	@Test
	public void testTermRegex() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today group by tk having term() =~ 'a*'");
	}

	@Test
	public void testMetricIs() {
		verify(ImmutableSet.of(ORGANIC_TK, ORGANIC_OJC), "from organic yesterday today group by tk having ojc = 1");
	}

	@Test
	public void testMetricIsnt() {
		verify(ImmutableSet.of(ORGANIC_TK, ORGANIC_OJC), "from organic yesterday today group by tk having ojc != 1");
	}

	@Test
	public void testGt() {
		verify(ImmutableSet.of(ORGANIC_TK, ORGANIC_OJC), "from organic yesterday today group by tk having ojc > 1");
	}

	@Test
	public void testGte() {
		verify(ImmutableSet.of(ORGANIC_TK, ORGANIC_OJC), "from organic yesterday today group by tk having ojc >= 1");
	}

	@Test
	public void testLt() {
		verify(ImmutableSet.of(ORGANIC_TK, ORGANIC_OJC), "from organic yesterday today group by tk having ojc < 1");
	}

	@Test
	public void testLte() {
		verify(ImmutableSet.of(ORGANIC_TK, ORGANIC_OJC), "from organic yesterday today group by tk having ojc <= 1");
	}

	@Test
	public void testAnd() {
		verify(ImmutableSet.of(ORGANIC_TK, ORGANIC_OJC, ORGANIC_OJI), "from organic yesterday today group by tk having ojc <= 1 and oji >=1");
	}

	@Test
	public void testOr() {
		verify(ImmutableSet.of(ORGANIC_OJI, ORGANIC_OJC, ORGANIC_TK), "from organic yesterday today group by tk having ojc <= 1 or oji >=1");
	}

	@Test
	public void testNot() {
		verify(ImmutableSet.of(ORGANIC_OJC, ORGANIC_TK), "from organic yesterday today group by tk having !ojc = 1");
	}

	@Test
	public void testRegex() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today group by tk having tk =~ 'a*'");
	}

	@Test
	public void testAlways() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today group by tk having true");
	}

	@Test
	public void testNever() {
		verify(ImmutableSet.of(ORGANIC_TK), "from organic yesterday today group by tk having false");
	}

	@Test
	public void testIsDefaultGroup() {
		//TODO
	}

}

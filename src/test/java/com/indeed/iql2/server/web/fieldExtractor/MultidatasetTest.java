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

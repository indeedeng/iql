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

package com.indeed.iql2.language.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.net.util.Base64;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

@EqualsAndHashCode
@ToString
public class IntOrAction implements Action {
    public final FieldSet field;
    @ToString.Exclude // include sha1SummedTerms instead
    public final ImmutableSet<Long> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    @Getter(lazy=true)
    private final String sha1SummedTerms = sha1SummedTerms();

    public IntOrAction(final FieldSet field, final ImmutableSet<Long> terms, final int targetGroup, final int positiveGroup, final int negativeGroup) {
        this.field = field;
        this.terms = terms;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        for (final String dataset : field.datasets()) {
            validationHelper.validateIntField(dataset, field.datasetFieldName(dataset), errorCollector, this);
        }
    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction() {
        return new com.indeed.iql2.execution.actions.IntOrAction(
                field,
                terms,
                targetGroup,
                positiveGroup,
                negativeGroup
        );
    }

    private String sha1SummedTerms() {
        final MessageDigest sha1 = DigestUtils.getSha1Digest();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        for (final long term : terms) {
            byteBuffer.putLong(0, term);
            sha1.update(byteBuffer.array());
        }
        return Base64.encodeBase64URLSafeString(sha1.digest());
    }
}

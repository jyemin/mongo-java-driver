/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.expressions;

import com.mongodb.annotations.Immutable;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A branch of a switch expression.
 *
 * @see Expressions#branch(Expression, Expression)
 * @since 4.?
 */
@Immutable
public final class Branch {
    private final Expression caseExpr;
    private final Expression thenExpr;

    Branch(final Expression caseExpr, final Expression thenExpr) {
        this.caseExpr = notNull("case", caseExpr);
        this.thenExpr = notNull("then", thenExpr);
    }

    /**
     * Gets the case expression.
     *
     * @return the case expression
     */
    public Expression getCase() {
        return caseExpr;
    }

    /**
     * Gets the then expression.
     *
     * @return the then expression
     */
    public Expression getThen() {
        return thenExpr;
    }

    @Override
    public String toString() {
        return "Branch{"
                + "case=" + caseExpr
                + ", then=" + thenExpr
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Branch branch = (Branch) o;
        return Objects.equals(caseExpr, branch.caseExpr) && Objects.equals(thenExpr, branch.thenExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(caseExpr, thenExpr);
    }
}

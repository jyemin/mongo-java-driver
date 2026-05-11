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
package com.mongodb.mqlv2.ast;

public enum DatePart {
    YEAR("year"),
    MONTH("month"),
    DAY_OF_YEAR("dayOfYear"),
    DAY_OF_MONTH("dayOfMonth"),
    DAY_OF_WEEK("dayOfWeek"),
    HOUR("hour"),
    MINUTE("minute"),
    SECOND("second"),
    MILLISECOND("millisecond");

    private final String surface;

    DatePart(final String surface) {
        this.surface = surface;
    }

    public String surface() {
        return surface;
    }
}

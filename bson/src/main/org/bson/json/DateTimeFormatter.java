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

package org.bson.json;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

final class DateTimeFormatter {
    private static final FormatterImpl FORMATTER_IMPL =
            loadDateTimeFormatter("org.bson.json.DateTimeFormatter$DateTimeFormatterImpl");

    private static FormatterImpl loadDateTimeFormatter(final String className) {
        try {
            return (FormatterImpl) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            // this is unexpected as it means the class itself is not found
            throw new ExceptionInInitializerError(e);
        } catch (InstantiationException e) {
            // this is unexpected as it means the class can't be instantiated
            throw new ExceptionInInitializerError(e);
        } catch (IllegalAccessException e) {
            // this is unexpected as it means the no-args constructor isn't accessible
            throw new ExceptionInInitializerError(e);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        } catch (InvocationTargetException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static long parse(final String dateTimeString) {
        return FORMATTER_IMPL.parse(dateTimeString);
    }

    static String format(final long dateTime) {
        return FORMATTER_IMPL.format(dateTime);
    }

    private interface FormatterImpl {
        long parse(String dateTimeString);
        String format(long dateTime);
    }

    static class DateTimeFormatterImpl implements FormatterImpl {

        // if running on Java 8 or above then java.time.format.DateTimeFormatter will be available and initialization will succeed.
        // Otherwise it will fail.
        static {
            try {
                Class.forName("java.time.format.DateTimeFormatter");
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @Override
        public long parse(final String dateTimeString) {
            try {
                return ISO_OFFSET_DATE_TIME.parse(dateTimeString, new TemporalQuery<Instant>() {
                    @Override
                    public Instant queryFrom(final TemporalAccessor temporal) {
                        return Instant.from(temporal);
                    }
                }).toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        @Override
        public String format(final long dateTime) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateTime), ZoneId.of("Z")).format(ISO_OFFSET_DATE_TIME);
        }
    }

    private DateTimeFormatter() {
    }
}

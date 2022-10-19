/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.bson.codecs.jackson.samples;

import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public class AllTypesPojo {
    private ObjectId id;
    private int i;
    private long l;
    private double d;
    private boolean b;
    private Date date;
    private Decimal128 dec;
    private List<Integer> listOfInt;

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    public int getI() {
        return i;
    }

    public void setI(final int i) {
        this.i = i;
    }

    public long getL() {
        return l;
    }

    public void setL(final long l) {
        this.l = l;
    }

    public boolean isB() {
        return b;
    }

    public void setB(final boolean b) {
        this.b = b;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(final Date date) {
        this.date = date;
    }

    public List<Integer> getListOfInt() {
        return listOfInt;
    }

    public void setListOfInt(final List<Integer> listOfInt) {
        this.listOfInt = listOfInt;
    }

    public Decimal128 getDec() {
        return dec;
    }

    public void setDec(final Decimal128 dec) {
        this.dec = dec;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AllTypesPojo that = (AllTypesPojo) o;
        return i == that.i && l == that.l && Double.compare(that.d, d) == 0 && b == that.b && Objects.equals(id, that.id) && Objects.equals(date, that.date) && Objects.equals(dec, that.dec) && Objects.equals(listOfInt, that.listOfInt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, i, l, d, b, date, dec, listOfInt);
    }
}

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
 */

package org.mongodb.rs;

import org.mongodb.ServerAddress;
import org.mongodb.annotations.Immutable;

@Immutable
public class Node {

    private final ServerAddress address;
    private final float pingTime;
    private final boolean ok;
    private final int maxBSONObjectSize;

    Node(final float pingTime, final ServerAddress addr, final int maxBSONObjectSize, final boolean ok) {
        this.pingTime = pingTime;
        this.address = addr;
        this.maxBSONObjectSize = maxBSONObjectSize;
        this.ok = ok;
    }

    public boolean isOk() {
        return ok;
    }

    public int getMaxBSONObjectSize() {
        return maxBSONObjectSize;
    }

    public ServerAddress getServerAddress() {
        return getAddress();
    }

    public ServerAddress getAddress() {
        return address;
    }

    public float getPingTime() {
        return pingTime;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Node node = (Node) o;

        if (getMaxBSONObjectSize() != node.getMaxBSONObjectSize()) {
            return false;
        }
        if (isOk() != node.isOk()) {
            return false;
        }
        if (Float.compare(node.getPingTime(), getPingTime()) != 0) {
            return false;
        }
        if (!getAddress().equals(node.getAddress())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getAddress().hashCode();
        result = 31 * result + (getPingTime() != +0.0f ? Float.floatToIntBits(getPingTime()) : 0);
        result = 31 * result + (isOk() ? 1 : 0);
        result = 31 * result + getMaxBSONObjectSize();
        return result;
    }

    public String toJSON() {
        final StringBuilder buf = new StringBuilder();
        buf.append("{");
        buf.append("address:'").append(getAddress()).append("', ");
        buf.append("ok:").append(isOk()).append(", ");
        buf.append("ping:").append(getPingTime()).append(", ");
        buf.append("maxBSONObjectSize:").append(getMaxBSONObjectSize()).append(", ");
        buf.append("}");

        return buf.toString();
    }

    @Override
    public String toString() {
        return "Node{"
                + "address=" + address
                + ", pingTime=" + pingTime
                + ", ok=" + ok
                + ", maxBSONObjectSize="
                + maxBSONObjectSize
                + '}';
    }
}

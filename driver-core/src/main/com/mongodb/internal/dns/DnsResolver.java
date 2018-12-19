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

package com.mongodb.internal.dns;

import java.util.List;

/**
 * Utility interface for resolving SRV and TXT records.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public interface DnsResolver {
    // The format of SRV record is
    // priority weight port target.
    // e.g.
    // 0 5 5060 example.com.
    // The priority and weight are ignored, and we just concatenate the host (after removing the ending '.') and port with a
    // ':' in between, as expected by ServerAddress
    // It's required that the srvHost has at least three parts (e.g. foo.bar.baz) and that all of the resolved hosts have a parent
    // domain equal to the domain of the srvHost.
    List<String> resolveHostFromSrvRecords(String srvHost);

    // A TXT record is just a string
    // We require each to be one or more query parameters for a MongoDB connection string.
    // Here we concatenate TXT records together with a '&' separator as required by connection strings
    String resolveAdditionalQueryParametersFromTxtRecords(String host);
}

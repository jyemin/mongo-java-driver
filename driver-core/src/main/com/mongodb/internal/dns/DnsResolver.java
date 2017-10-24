/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.dns;

import com.mongodb.MongoClientException;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Utility class for resolving SRV and TXT records.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class DnsResolver {

    // The format of SRV record is
    // priority weight port target.
    // e.g.
    // 0 5 5060 example.com.
    // The priority and weight are ignored, and we just concatenate the host (after removing the ending '.') and port with a
    // ':' in between, as expected by ServerAddress
    public static List<String> resolveHostFromSrvRecords(final String host) {
        List<String> hosts = new ArrayList<String>();
        try {
            InitialDirContext dirContext = createDnsDirContext();
            Attributes attributes = dirContext.getAttributes("_mongodb._tcp." + host, new String[]{"SRV"});
            Attribute attribute = attributes.get("SRV");
            if (attribute == null) {
                throw new MongoClientException("No SRV record available for host " + host);
            }
            NamingEnumeration<?> srvRecordEnumeration = attribute.getAll();
            while (srvRecordEnumeration.hasMore()) {
                String srvRecord = (String) srvRecordEnumeration.next();
                String[] split = srvRecord.split(" ");
                String resolvedHost = split[3].endsWith(".") ? split[3].substring(0, split[3].length() - 1) : split[3];
                hosts.add(resolvedHost + ":" + split[2]);
            }
        } catch (NamingException e) {
            throw new MongoClientException("Unable to look up SRV record for host " + host, e);
        }
        return hosts;
    }

    // A TXT record is just a string
    // We require each to be one or more query parameters for a MongoDB connection string.
    // Here we concatenate TXT records together with a '&' separator as required by connection strings
    public static String resolveAdditionalQueryParametersFromTxtRecords(final String host) {
        String additionalQueryParameters = "";
        try {
            InitialDirContext dirContext = createDnsDirContext();
            Attributes attributes = dirContext.getAttributes(host, new String[]{"TXT"});
            Attribute attribute = attributes.get("TXT");
            if (attribute != null) {
                StringBuilder additionalQueryParametersBuilder = new StringBuilder();
                NamingEnumeration<?> txtRecordEnumeration = attribute.getAll();
                while (txtRecordEnumeration.hasMore()) {
                    String txtRecord = (String) txtRecordEnumeration.next();
                    if (!additionalQueryParametersBuilder.toString().isEmpty()) {
                        additionalQueryParametersBuilder.append('&');
                    }
                    additionalQueryParametersBuilder.append(txtRecord);
                }
                additionalQueryParameters = additionalQueryParametersBuilder.toString();
            }
        } catch (NamingException e) {
            throw new MongoClientException("Unable to look up TXT record for host " + host, e);
        }
        return additionalQueryParameters;
    }

    // It's unfortunate that we take a runtime dependency on com.sun.jndi.dns.DnsContextFactory.
    // This is not guaranteed to work on all JVMs but in practice is expected to work on most.
    private static InitialDirContext createDnsDirContext() {
        Hashtable<String, String> envProps = new Hashtable<String, String>();
        envProps.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        try {
            return new InitialDirContext(envProps);
        } catch (NamingException e) {
            throw new MongoClientException("Unable to create JNDI context for resolving SRV records. "
                    + "The 'com.sun.jndi.dns.DnsContextFactory' class is not available in this JRE", e);
        }
    }

    private DnsResolver() {}
}

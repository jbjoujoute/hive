/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.contrib.metastore.hooks.consul;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.hooks.URIResolverHook;
import org.apache.http.client.utils.URIBuilder;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Looks up from consul to return a list of current metastore uri's in form thrift://host:port
 *
 * Input uri is of the form consul://consul-host:consul-port/service-name
 */
public class ConsulURLHook implements URIResolverHook {

    static final protected Log LOG = LogFactory.getLog("hive.metastore");

    private static final GsonBuilder GSON_BUILDER = new GsonBuilder();

    private static final ThreadLocal<Gson> GSON = new ThreadLocal<Gson>() {
        @Override
        protected Gson initialValue() {
            return GSON_BUILDER.create();
        }
    };

    public static Gson getGson() {
        return GSON.get();
    }

    private String agentAddress;
    private HttpTransport httpTransport = new HttpTransport();

    @Override
    public List<URI> resolveURI(URI uri) throws HiveMetaException {
        String scheme = uri.getScheme();
        if (!scheme.equalsIgnoreCase("consul")) {
            throw new IllegalArgumentException("Invalid scheme, please use consul://consul-host:consul-port/service-name");
        }

        LOG.info("Resolving consul uri : " + uri);
        String consulHost = uri.getHost();
        String service = uri.getPath().substring(1);  //strip leading slash
        int consulPort = uri.getPort();

        // check that agentHost has scheme or not
        if (consulHost == "") {
            throw new IllegalArgumentException("Unspecified consul host, please use consul://consul-host:consul-port/service-name");
        }
        if (consulPort == -1) {
            throw new IllegalArgumentException("Unspecified consul port, please use consul://consul-host:consul-port/service-name");
        }
        if (service == "") {
            throw new IllegalArgumentException("Unspecified consul service, please use consul://consul-host:consul-port/service-name");
        }

        String url = assembleUrl(consulHost, consulPort, service);
        RawResponse rawResponse = httpTransport.makeGetRequest(url);
        Response<List<HealthService>> healthyServices;
        if (rawResponse.getStatusCode() == 200) {
            List<HealthService> value = getGson().fromJson(rawResponse.getContent(),
                    new TypeToken<List<HealthService>>() {
                    }.getType());
            healthyServices = new Response<List<HealthService>>(value, rawResponse);
        } else {
            throw new HiveMetaException(rawResponse.getStatusMessage());
        }

        List<URI> thriftUris = new ArrayList<URI>();

        LOG.info(String.format("Querying Consul (%s) for service %s", consulHost, service));
        for(Iterator<HealthService> i = healthyServices.getValue().iterator(); i.hasNext(); ) {
            HealthService hn = i.next();
            URI thriftUri = UriBuilder.fromUri("")
                    .scheme("thrift")
                    .host(hn.getNode().getAddress())
                    .port(hn.getService().getPort())
                    .build();
            thriftUris.add(thriftUri);
        }
        if(thriftUris.size() == 0) {
            throw new IllegalArgumentException("There is no healthy nodes in Consul for the service: " + service);
        }
        Collections.shuffle(thriftUris);
        return thriftUris;
    }

    public static String assembleUrl(String host, int port, String service) throws HiveMetaException {
        try {
            URIBuilder builder = new URIBuilder("/v1/health/service/" + service);
            builder.setHost(host)
                    .setPort(port)
                    .setParameter("passing", null)
                    .setScheme("http");
            return builder.build().toASCIIString();
        } catch (Exception e) {
            throw new HiveMetaException("Can't encode url", e);
        }
    }
}

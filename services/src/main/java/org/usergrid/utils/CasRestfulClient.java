/*
 * Copyright 2013 capacman.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.usergrid.utils;

/**
 *
 * @author capacman
 */
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CasRestfulClient {

    static final Logger LOG = LoggerFactory.getLogger(CasRestfulClient.class);

    static public String getServiceTicket(String server, String ticketGrantingTicket, String service) {
        if (ticketGrantingTicket == null || ticketGrantingTicket.isEmpty()) {
            return null;
        }
        HttpClient client = new HttpClient();
        PostMethod post = new PostMethod(server + "/" + ticketGrantingTicket);
        post.setRequestBody(new NameValuePair[]{new NameValuePair("service", service)});

        try {
            client.executeMethod(post);
            String response = post.getResponseBodyAsString();
            switch (post.getStatusCode()) {
                case 200:
                    return response;
                default:
                    LOG.warn("Invalid response code ( {} ) from CAS server!", post.getStatusCode());
                    LOG.info("Response (1k): {}", response.substring(0, Math.min(1024, response.length())));
                    break;
            }
        } catch (final IOException e) {
            LOG.warn(e.getMessage());
        } finally {
            post.releaseConnection();
        }
        return null;
    }

    static public String getTicketGrantingTicket(String server, String username, String password) {
        HttpClient client = new HttpClient();
        PostMethod post = new PostMethod(server);
        post.setRequestBody(new NameValuePair[]{new NameValuePair("username", username), new NameValuePair("password", password)});
        try {
            client.executeMethod(post);
            String response = post.getResponseBodyAsString();
            switch (post.getStatusCode()) {
                case 201:
                    Matcher matcher = Pattern.compile(".*action=\".*/(.*?)\".*").matcher(response);
                    if (matcher.matches()) {
                        return matcher.group(1);

                    }
                    LOG.warn("Successful ticket granting request, but no ticket found!");
                    LOG.info("Response (1k): {}", response.substring(0, Math.min(1024, response.length())));
                    break;
                default:
                    LOG.warn("Invalid response code ({}) from CAS server!", post.getStatusCode());
                    LOG.info("Response: {}", response);
                    break;
            }
        } catch (final IOException e) {
            LOG.warn(e.getMessage());
        } finally {
            post.releaseConnection();
        }
        return null;
    }

    static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);

        }
    }

    static public void getServiceCall(String service, String serviceTicket) {
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(service);
        method.setQueryString(new NameValuePair[]{new NameValuePair("ticket", serviceTicket)});

        try {
            client.executeMethod(method);
            String response = method.getResponseBodyAsString();
            switch (method.getStatusCode()) {
                case 200:
                    LOG.info("Response: {}", response);
                    break;
                default:
                    LOG.warn("Invalid response code ({}) from CAS server!", method.getStatusCode());
                    LOG.info("Response: {}", response);
                    break;
            }
        } catch (final IOException e) {
            LOG.warn(e.getMessage());
        } finally {
            method.releaseConnection();
        }
    }

    static public void logout(String server, String ticketGrantingTicket) {
        HttpClient client = new HttpClient();
        final String endPoint = server + ticketGrantingTicket;
        DeleteMethod method = new DeleteMethod(endPoint);
        try {
            client.executeMethod(method);
            String response = method.getResponseBodyAsString();
            switch (method.getStatusCode()) {
                case 200:
                    LOG.info("Logged out");
                    break;
                default:
                    LOG.warn("Invalid response code ({}) from CAS server!", method.getStatusCode());
                    LOG.info("Response: {}", response);
                    break;
            }
        } catch (final IOException e) {
            LOG.warn(e.getMessage());
        } finally {
            method.releaseConnection();
        }
    }
}

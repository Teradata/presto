/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.tests.queryinfo;

import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.server.BasicQueryInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.http.HttpStatus.SC_GONE;
import static org.apache.http.HttpStatus.SC_OK;

/**
 * Implementation of {@link QueryInfoClient} based on Apache http client.
 */
public class HttpQueryInfoClient
        implements QueryInfoClient
{
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String baseUrl;

    public HttpQueryInfoClient(HttpClient httpClient, ObjectMapper objectMapper, String baseUrl)
    {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.baseUrl = baseUrl;
    }

    @Override
    public List<BasicQueryInfo> getBasicInfoForAllQueries()
    {
        HttpGet request = new HttpGet(baseUrl + "/v1/query");
        try {
            return httpClient.execute(request, new GetInfoForAllQueriesResponseHandler());
        }
        catch (IOException e) {
            throw new RuntimeException("http error", e);
        }
    }

    @Override
    public Optional<QueryStats> getQueryStats(String queryId)
    {
        HttpGet request = new HttpGet(baseUrl + "/v1/query/" + queryId);
        try {
            return httpClient.execute(request, new GetQueryStatsResponseHandler());
        }
        catch (IOException e) {
            throw new RuntimeException("http error", e);
        }
    }

    private final class GetInfoForAllQueriesResponseHandler
            implements ResponseHandler<List<BasicQueryInfo>>
    {
        @Override
        public List<BasicQueryInfo> handleResponse(HttpResponse response)
                throws IOException
        {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw new HttpResponseException(response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
            }

            HttpEntity entity = checkGetEntity(response);
            List<BasicQueryInfo> basicQueryInfos = objectMapper.readValue(entity.getContent(), new TypeReference<List<BasicQueryInfo>>() {});
            return basicQueryInfos;
        }
    }

    private final class GetQueryStatsResponseHandler
            implements ResponseHandler<Optional<QueryStats>>
    {
        @Override
        public Optional<QueryStats> handleResponse(HttpResponse response)
                throws IOException
        {
            if (response.getStatusLine().getStatusCode() == SC_GONE) {
                return Optional.empty();
            }
            else if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw new HttpResponseException(response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
            }

            HttpEntity entity = checkGetEntity(response);
            JsonNode rootNode = objectMapper.readTree(entity.getContent());
            JsonNode queryStatsNode = rootNode.get("queryStats");
            if (queryStatsNode == null) {
                return Optional.empty();
            }
            QueryStats queryStats = objectMapper.treeToValue(queryStatsNode, QueryStats.class);
            return Optional.of(queryStats);
        }
    }

    private HttpEntity checkGetEntity(HttpResponse response)
            throws ClientProtocolException
    {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new ClientProtocolException("Response contained no content");
        }
        return entity;
    }
}

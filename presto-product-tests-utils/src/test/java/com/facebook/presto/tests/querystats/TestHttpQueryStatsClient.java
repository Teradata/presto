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

package com.facebook.presto.tests.querystats;

import com.facebook.presto.execution.QueryStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.io.Resources;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.ObjectMapperProvider;
import org.assertj.core.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Stubber;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Optional;

import static com.google.common.base.Charsets.UTF_8;

@Test(singleThreaded = true)
public class TestHttpQueryStatsClient
{
    private static final URI BASE_URL = URI.create("http://presto.host");
    @Mock private HttpClient httpClient;
    private HttpQueryStatsClient queryStatsClient;
    @Captor private ArgumentCaptor<Request> requestCaptor;

    private static final String SINGLE_QUERY_INFO = resourceAsString("com/facebook/presto/client/single_query_info_response.json");

    private static String resourceAsString(String resourcePath)
    {
        try {
            URL resourceUrl = Resources.getResource(resourcePath);
            return Resources.toString(resourceUrl, UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        MockitoAnnotations.initMocks(this);
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        this.queryStatsClient = new HttpQueryStatsClient(httpClient, objectMapper, BASE_URL);
    }

    @Test
    public void testGetInfoForQuery()
            throws Exception
    {
        mockHttpAnswer(SINGLE_QUERY_INFO).when(httpClient).execute(requestCaptor.capture(), Matchers.any(ResponseHandler.class));
        Optional<QueryStats> infoForQuery = queryStatsClient.getQueryStats("20150505_160116_00005_sdzex");
        Assertions.assertThat(infoForQuery).isPresent();
        Assertions.assertThat(infoForQuery.get().getTotalCpuTime().getValue()).isEqualTo(1.19);
    }

    @Test
    public void testGetInfoForUnknownQuery()
            throws Exception
    {
        mockErrorHttpAnswer(HttpStatus.GONE).when(httpClient).execute(requestCaptor.capture(), Matchers.any(ResponseHandler.class));
        Optional<QueryStats> infoForQuery = queryStatsClient.getQueryStats("20150505_160116_00005_sdzex");
        Assertions.assertThat(infoForQuery).isEmpty();
    }

    private Stubber mockHttpAnswer(String answerJson)
            throws IOException
    {
        return Mockito.doAnswer(invocationOnMock -> {
            Request request = (Request) invocationOnMock.getArguments()[0];
            ResponseHandler responseHandler = (ResponseHandler) invocationOnMock.getArguments()[1];
            TestingResponse response = new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(), answerJson.getBytes());
            return responseHandler.handle(request, response);
        });
    }

    private Stubber mockErrorHttpAnswer(HttpStatus statusCode)
            throws IOException
    {
        return Mockito.doAnswer(invocationOnMock -> {
            Request request = (Request) invocationOnMock.getArguments()[0];
            ResponseHandler responseHandler = (ResponseHandler) invocationOnMock.getArguments()[1];
            TestingResponse response = new TestingResponse(statusCode, ImmutableListMultimap.of(), new byte[0]);
            return responseHandler.handle(request, response);
        });
    }
}

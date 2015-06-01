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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.airlift.json.ObjectMapperProvider;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.http.HttpStatus.SC_GONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.MockitoAnnotations.initMocks;

@Test(singleThreaded = true)
public class TestHttpQueryInfoClient
{
    private static final String BASE_URL = "http://presto.host";
    @Mock private HttpClient httpClient;
    private HttpQueryInfoClient queryInfoClient;
    @Captor private ArgumentCaptor<HttpGet> requestCaptor;

    private static final String SINGLE_QUERY_INFO = resourceAsString("com/facebook/presto/tests/queryinfo/single_query_info_response.json");
    private static final String MULTIPLE_BASIC_QUERY_INFOS = resourceAsString("com/facebook/presto/tests/queryinfo/multiple_basic_query_infos_response.json");

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
        initMocks(this);
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        this.queryInfoClient = new HttpQueryInfoClient(httpClient, objectMapper, BASE_URL);
    }

    @Test
    public void testGetInfoForAllQueries()
            throws Exception
    {
        mockHttpAnswer(MULTIPLE_BASIC_QUERY_INFOS).when(httpClient).execute(requestCaptor.capture(), any(ResponseHandler.class));
        List<BasicQueryInfo> infoForAllQueries = queryInfoClient.getBasicInfoForAllQueries();
        assertThat(infoForAllQueries).hasSize(7);
        assertThat(queryIds(infoForAllQueries)).containsExactly(
                "20150505_142801_00000_sdzex",
                "20150505_160101_00003_sdzex",
                "20150505_142803_00001_sdzex",
                "20150505_160125_00006_sdzex",
                "20150505_160116_00005_sdzex",
                "20150505_160025_00002_sdzex",
                "20150505_160112_00004_sdzex");
    }

    @Test
    public void testGetInfoForQuery()
            throws Exception
    {
        mockHttpAnswer(SINGLE_QUERY_INFO).when(httpClient).execute(requestCaptor.capture(), any(ResponseHandler.class));
        Optional<QueryStats> infoForQuery = queryInfoClient.getQueryStats("20150505_160116_00005_sdzex");
        assertThat(infoForQuery).isPresent();
        assertThat(infoForQuery.get().getTotalCpuTime().getValue()).isEqualTo(1.19);
    }

    @Test
    public void testGetInfoForUnknownQuery()
            throws Exception
    {
        mockErrorHttpAnswer(SC_GONE).when(httpClient).execute(requestCaptor.capture(), any(ResponseHandler.class));
        Optional<QueryStats> infoForQuery = queryInfoClient.getQueryStats("20150505_160116_00005_sdzex");
        assertThat(infoForQuery).isEmpty();
    }

    private List<String> queryIds(List<BasicQueryInfo> infoForAllQueries)
    {
        return infoForAllQueries.stream().map((bqi) -> bqi.getQueryId().getId()).collect(toList());
    }

    private Stubber mockHttpAnswer(String answerJson)
            throws IOException
    {
        return Mockito.doAnswer(invocationOnMock -> {
            ResponseHandler responseHandler = (ResponseHandler) invocationOnMock.getArguments()[1];
            BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK");
            response.setEntity(new StringEntity(answerJson));
            return responseHandler.handleResponse(response);
        });
    }

    private Stubber mockErrorHttpAnswer(int statusCode)
            throws IOException
    {
        return Mockito.doAnswer(invocationOnMock -> {
            ResponseHandler responseHandler = (ResponseHandler) invocationOnMock.getArguments()[1];
            BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, statusCode, "error");
            return responseHandler.handleResponse(response);
        });
    }
}

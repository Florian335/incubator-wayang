/*
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

package org.apache.wayang.basic.operators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.OptionalDouble;

public class RestAPISourceTest {
    private static final Logger logger = LogManager.getLogger(RestAPISourceTest.class);
    private static final String TEST_API_URL = "https://jsonplaceholder.typicode.com/posts";
    private RestAPISource restAPISource;

    @BeforeEach
    public void setUp() {
        restAPISource = new RestAPISource(TEST_API_URL, "GET", "");
    }

    @Test
    public void testFetchDataFromAPI() {
        JSONArray response = restAPISource.fetchDataFromAPI();
        Assertions.assertNotNull(response, "Response should not be null");
        Assertions.assertTrue(response.length() > 0, "Response should contain some data");

        logger.info("Fetched {} records from API", response.length());

        JSONObject firstObject = response.getJSONObject(0);
        Assertions.assertTrue(firstObject.has("userId"), "JSON object should contain 'userId' field");
        Assertions.assertTrue(firstObject.has("id"), "JSON object should contain 'id' field");
        Assertions.assertTrue(firstObject.has("title"), "JSON object should contain 'title' field");
        Assertions.assertTrue(firstObject.has("body"), "JSON object should contain 'body' field");

        logger.info("First object from API response: {}", firstObject.toString());
    }

    @Test
    public void testCachedResponse() {
        JSONArray firstResponse = restAPISource.fetchDataFromAPI();
        JSONArray secondResponse = restAPISource.fetchDataFromAPI();
        Assertions.assertSame(firstResponse, secondResponse, "Subsequent fetch should return cached response");
    }

    @Test
    public void testEstimateBytesPerLine() {
        restAPISource.fetchDataFromAPI();
        OptionalDouble bytesPerLine = restAPISource.estimateBytesPerLine();
        Assertions.assertTrue(bytesPerLine.isPresent(), "Bytes per line estimate should be present");
        logger.info("Estimated bytes per line: {}", bytesPerLine.getAsDouble());
        Assertions.assertTrue(bytesPerLine.getAsDouble() > 0, "Bytes per line estimate should be greater than zero");
    }
}

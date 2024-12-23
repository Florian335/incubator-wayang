// /*
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// package org.apache.wayang.basic.operators;

// // import org.apache.wayang.basic.operators.RestAPISource;
// // import org.apache.wayang.core.api.WayangContext;
// import org.json.JSONArray;
// // import org.json.JSONObject;
// import org.junit.Assert;
// import org.junit.Test;

// public class RestAPISourceTest {

//     @Test
//     public void testFetchDataFromJsonPlaceholderAPI() {
//         String apiUrl = "https://jsonplaceholder.typicode.com/posts";
//         String apiMethod = "GET";
//         String headers = "";

//         // Create the RestAPISource
//         RestAPISource restAPISource = new RestAPISource(apiUrl, apiMethod, headers);

//         // Fetch data from the API
//         JSONArray responseData = restAPISource.fetchDataFromAPI();

//         // Validate the response
//         Assert.assertNotNull("The response should not be null.", responseData);
//         Assert.assertTrue("The response should be a JSONArray.", responseData instanceof JSONArray);
//         Assert.assertTrue("The response should contain at least one object.", responseData.length() > 0);

//         // int entriesToPrint = Math.min(10, responseData.length());
//         // for (int i = 0; i < entriesToPrint; i++) {
//         //     JSONObject object = responseData.getJSONObject(i);
//         //     System.out.println("Entry " + (i + 1) + ": " + object.toString());
//         // }
//     }

//     @Test
//     public void testCachedResponse() {
//         String apiUrl = "https://jsonplaceholder.typicode.com/posts";
//         String apiMethod = "GET";
//         String headers = ""; // No headers needed

//         // Create the RestAPISource
//         RestAPISource restAPISource = new RestAPISource(apiUrl, apiMethod, headers);

//         // Fetch data the first time
//         JSONArray firstResponse = restAPISource.fetchDataFromAPI();

//         // Fetch data the second time (should use cache)
//         JSONArray cachedResponse = restAPISource.fetchDataFromAPI();

//         Assert.assertSame("The cached response should be the same instance as the first response.", firstResponse, cachedResponse);
//     }
// }

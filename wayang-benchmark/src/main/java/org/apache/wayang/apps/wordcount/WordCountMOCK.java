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

package org.apache.wayang.apps.wordcount;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class WordCountMOCK {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WordCountMOCK.class);

    public static void main(String[] args) {
        // Initialize Wayang context and plan builder
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(org.apache.wayang.java.Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(WordCountMOCK.class);

        try {
            Collection<Tuple2<String, Integer>> wordCounts = runWithMockData(planBuilder);

            // Print the word counts
            printWordCounts(wordCounts);
        } catch (Exception e) {
            log.error("Error during WordCount processing: {}", e.getMessage(), e);
        }
    }

    /**
     * Runs the pipeline using mock API data for testing.
     */
    private static Collection<Tuple2<String, Integer>> runWithMockData(JavaPlanBuilder planBuilder) {
        return planBuilder
                .loadCollection(mockAPIResponse())
                .flatMap(WordCountMOCK::extractBodiesFromResponse) // Fixed to match processing pipeline
                .flatMap(WordCountMOCK::splitIntoWords)            // Process words
                .map(word -> new Tuple2<>(word.toLowerCase(), 1))  // Convert to key-value pairs
                .reduceByKey(
                        tuple -> tuple.field0,                     // Group by the word (field0)
                        (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1) // Sum counts
                )
                .collect();
    }

    /**
     * Mock API response for testing.
     */
    private static Collection<JSONObject> mockAPIResponse() {
        JSONArray jsonArray = new JSONArray("[{\"body\":\"This is a test body 1\"}, {\"body\":\"This is a test body 2\"}]");
        List<JSONObject> mockCollection = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            mockCollection.add(jsonArray.getJSONObject(i));
        }
        return mockCollection;
    }

    /**
     * Extracts the body field from the API response.
     */
    private static List<String> extractBodiesFromResponse(JSONObject jsonObject) {
        List<String> extractedBodies = new ArrayList<>();
        String body = jsonObject.optString("body", "").trim();
        if (!body.isEmpty()) {
            extractedBodies.add(body);
        }
        log.debug("Extracted body: {}", body); // Debug log for body extraction
        return extractedBodies;
    }

    /**
     * Splits lines of text into words.
     */
    private static List<String> splitIntoWords(String line) {
        List<String> words = java.util.Arrays.stream(line.split("\\W+"))
                .filter(word -> !word.isEmpty())
                .collect(Collectors.toList());
        log.debug("Split line into words: {}", words); // Debug log for word splitting
        return words;
    }

    /**
     * Prints the final word counts.
     */
    private static void printWordCounts(Collection<Tuple2<String, Integer>> wordCounts) {
        log.info("Final Word Counts:");
        wordCounts.stream()
                .sorted((a, b) -> b.field1.compareTo(a.field1)) // Sort by count descending
                .forEach(tuple -> log.info("{}: {}", tuple.field0, tuple.field1));
    }
}

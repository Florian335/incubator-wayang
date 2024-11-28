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
import org.apache.wayang.java.Java;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;


public class WordCountREST {

    private static final Logger log = LoggerFactory.getLogger(WordCountREST.class);

    public static void main(String[] args) {
        // Load configuration from .properties file
        Properties properties = new Properties();
        String apiUrl = null;
        System.out.println("Current working directory: " + System.getProperty("user.dir"));

        try (FileInputStream fis = new FileInputStream("/home/flo/incubator-wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/wordcount/config.properties")) {
            properties.load(fis);
            apiUrl = properties.getProperty("api_url");
            if (apiUrl == null || apiUrl.isEmpty()) {
                throw new IllegalArgumentException("API URL is not specified in the properties file.");
            }
        } catch (IOException e) {
            log.error("Error loading configuration file: {}", e.getMessage(), e);
            return;
        }

        // Initialize Wayang context and plan builder
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(WordCountREST.class);

        try {
            Collection<Tuple2<String, Integer>> wordCounts = runwithRESTAPIData(planBuilder, apiUrl);

            // Print the word counts
            printWordCounts(wordCounts);
        } catch (Exception e) {
            log.error("Error during WordCount processing: {}", e.getMessage(), e);
        }
    }

    private static Collection<Tuple2<String, Integer>> runwithRESTAPIData(JavaPlanBuilder planBuilder, String apiUrl) {
        String apiMethod = "GET";
        String headers = ""; // Add necessary headers here if required

        Collection<JSONArray> rawResponse = planBuilder.readRestAPISource(apiUrl, apiMethod, headers).collect();
        // log.info("Raw API Response: {}", rawResponse);

        List<JSONObject> extractedJsonObjects = new ArrayList<>();
        for (Object response : rawResponse) {
            extractedJsonObjects.add((JSONObject) response); // Cast and add directly
        }

        List<String> bodies = extractedJsonObjects.stream()
            .map(jsonObject -> jsonObject.optString("body", "").trim())
            .filter(body -> !body.isEmpty())
            .collect(Collectors.toList());
        // log.info("Extracted Bodies: {}", bodies);

        // Split bodies into words
        List<String> words = bodies.stream()
            .flatMap(body -> splitIntoWords(body).stream())
            .collect(Collectors.toList());
        // log.info("Split Words: {}", words);

        // Map words to (word, 1)
        List<Tuple2<String, Integer>> wordTuples = words.stream()
            .map(word -> new Tuple2<>(word.toLowerCase(), 1))
            .collect(Collectors.toList());
        // log.info("Word Tuples: {}", wordTuples);

        List<Tuple2<String, Integer>> wordCounts = wordTuples.stream()
            .collect(Collectors.groupingBy(
                tuple -> tuple.field0,
                Collectors.summingInt(tuple -> tuple.field1)
            ))
            .entrySet().stream()
            .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
        // log.info("Final Word Counts: {}", wordCounts);

        return wordCounts;
    }

    public static List<String> splitIntoWords(String line) {
        return java.util.Arrays.stream(line.split("\\W+")) // Split on non-word characters
                .filter(word -> !word.isEmpty())           // Remove empty words
                .collect(Collectors.toList());
    }

    private static void printWordCounts(Collection<Tuple2<String, Integer>> wordCounts) {
        log.info("Final Word Counts:");
        wordCounts.stream()
                .sorted((a, b) -> b.field1.compareTo(a.field1)) // Sort by count descending
                .forEach(tuple -> log.info("{}: {}", tuple.field0, tuple.field1));
    }
}

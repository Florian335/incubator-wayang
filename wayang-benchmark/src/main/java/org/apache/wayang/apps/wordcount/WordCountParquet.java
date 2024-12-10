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
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import java.util.Collection;
import java.util.Date;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountParquet {
        private static final Logger log = LoggerFactory.getLogger(WordCountParquet.class);
        private static final String LOG_FILE_PATH = "parquet-performance.json";

        public static void logtoJSON(String stepname, Double latencyseconds, Double executiontime){
                try {
                JSONObject logrecord = new JSONObject();
                logrecord.put("timestamp", Date.from(Instant.now()).toString());
                logrecord.put("step",stepname);
                logrecord.put("execution_time_seconds", executiontime);

                appendlogtofile(logrecord);
                } catch (Exception e) {
                log.error("Unable to add data to JSON: {}", e.getMessage(),e);
                }
        }

        public static void logQueryTime(long starttime, long endtime, String stepname){
                Double executiontimeseconds = (endtime - starttime) / 1000.0;
                logtoJSON(stepname, null, executiontimeseconds);
        }

        private static void appendlogtofile(JSONObject logrecord) throws IOException {
                JSONArray existinglogs;

                if (Files.exists(Paths.get(LOG_FILE_PATH))){
                String content = new String(Files.readAllBytes(Paths.get(LOG_FILE_PATH)));
                if (!content.isEmpty()){
                        existinglogs = new JSONArray(content);
                } else {
                        existinglogs = new JSONArray();
                }
                } else {
                existinglogs = new JSONArray();
                }

                existinglogs.put(logrecord);
                Files.write(
                Paths.get(LOG_FILE_PATH),
                existinglogs.toString(4).getBytes(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
                );
        }

    public static void main(String[] args){

        // Settings
        String inputUrl = "/Users/flo/github/rp-2024/incubator-wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/wordcount/text.parquet";

        // Get a plan builder.
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(WordCountParquet.class);

        // Start building the WayangPlan.
        long starttime_f = System.currentTimeMillis();
        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                // Read the text file.
                .readParquetFile(inputUrl).withName("Load file")

                // Split each line by non-word characters.
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withSelectivity(10, 100, 0.9)
                .withName("Split words")

                // Filter empty tokens.
                .filter(token -> !token.isEmpty())
                .withSelectivity(0.99, 0.99, 0.99)
                .withName("Filter empty words")

                // Attach counter to each word.
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                .withName("Add counters")

                // Execute the plan and collect the results.
                .collect();
        
        long endtime_f = System.currentTimeMillis();
        logQueryTime(starttime_f, endtime_f, "WordCount Query");

        System.out.println(wordcounts);
    }
}
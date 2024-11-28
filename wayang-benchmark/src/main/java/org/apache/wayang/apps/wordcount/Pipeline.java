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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.ChronoUnit;
import java.time.LocalDate;
import java.time.YearMonth;

class ForecastResult {
    private final float totalFTEs;
    private final int capacity;

    public ForecastResult(float totalFTEs, int capacity) {
        this.totalFTEs = totalFTEs;
        this.capacity = capacity;
    }

    public float getTotalFTEs() {
        return totalFTEs;
    }

    public int getCapacity() {
        return capacity;
    }
}

public class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    private static String forecastUser;
    private static String forecastToken;
    private static String hubspotToken;

    public static void main(String[] args) {
        Properties properties = new Properties();
        String configFilePath = "/home/flo/incubator-wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/wordcount/config.properties";

        LocalDateTime today = LocalDateTime.now()
                .with(TemporalAdjusters.firstDayOfNextMonth())
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String monthToday = today.format(formatter);

        LocalDateTime futureDate = today.plus(548, ChronoUnit.DAYS);
        String month1y = futureDate.format(formatter);

        try (FileInputStream fis = new FileInputStream(configFilePath)) {
            properties.load(fis);
            forecastUser = properties.getProperty("Forecast_user_agent");
            forecastToken = properties.getProperty("Forecast");
            hubspotToken = properties.getProperty("Hubspot");

            if (forecastUser == null || forecastToken == null || hubspotToken == null) {
                log.error("Missing required properties in configuration file.");
                return;
            }
        } catch (IOException e) {
            log.error("Error loading configuration file: {}", e.getMessage(), e);
            return;
        }

        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Pipelines")
                .withUdfJarOf(Pipeline.class);

        String urlForecast = String.format(
                "https://api.forecastapp.com/aggregate/project_export?timeframe_type=monthly&timeframe=custom&starting=%s&ending=%s",
                monthToday, month1y
        );

        String urlHubspot = "https://api.hubapi.com/crm/v3/objects/deals?limit=100&properties=start_date,end_date,hs_deal_stage_probability,fte_s_";

        try {
            ForecastResult forecastResult = ForecastPipeline(planBuilder, urlForecast);
            double totalFTEsHubspot = HubspotPipeline(planBuilder, urlHubspot, monthToday);
            log.info("Pipeline FTEs: {}", totalFTEsHubspot);

            int capacity = forecastResult.getCapacity();
            log.info("Capacity: {}", capacity);
            float totalFTEsForecast = forecastResult.getTotalFTEs();
            log.info("Committed FTEs: {}", totalFTEsForecast);

            double remainingCapacity = capacity - (totalFTEsHubspot + totalFTEsForecast);

            if (remainingCapacity < 0) {
                log.warn("Over capacity! Remaining capacity: {}", remainingCapacity);
            } else {
                log.info("Enough capacity. Remaining capacity: {}", remainingCapacity);
            }
        } catch (Exception e) {
            log.error("Error during Forecast API processing: {}", e.getMessage(), e);
        }
    }

    private static ForecastResult ForecastPipeline(JavaPlanBuilder planBuilder, String urlForecast) {
        String apiMethod = "GET";
        String headers = String.format("User-Agent: %s; Authorization: Bearer %s; Forecast-Account-ID: %s", forecastUser, forecastToken, forecastUser);

        log.info("Fetching data from {}", urlForecast);
        float totalFTEs = 0;
        int capacity = 0;

        try {
            Collection<JSONArray> rawResponse = planBuilder
                    .readRestAPISource(urlForecast, apiMethod, headers)
                    .collect();

            List<String> allowedRoles = Arrays.asList("DK", "US inc.");
            List<Float> committed_FTEs = new ArrayList<>();
            Set<String> filteredNames = new HashSet<>();

            for (Object element : rawResponse) {
                String role = ((JSONObject) element).optString("Roles", "");
                if (allowedRoles.contains(role)) {
                    filteredNames.add(((JSONObject) element).optString("Person", "Unknown"));
                    String dec2024 = ((JSONObject) element).optString("Dec 2024", "0");
                    try {
                        committed_FTEs.add(Float.parseFloat(dec2024) / 165);
                    } catch (NumberFormatException e) {
                        log.error("Invalid number for Dec 2024: {}", dec2024);
                    }
                }
            }

            totalFTEs = (float) committed_FTEs.stream()
                    .mapToDouble(Float::doubleValue)
                    .sum();
            capacity = filteredNames.size();

            log.info("Total FTEs from Forecast: {}", totalFTEs);
            log.info("Capacity from Forecast: {}", capacity);
        } catch (Exception e) {
            log.error("Error fetching data from Forecast API: {}", e.getMessage(), e);
        }

        return new ForecastResult(totalFTEs, capacity);
    }

    private static double HubspotPipeline(JavaPlanBuilder planBuilder, String urlHubspot, String monthToday) {
        String apiMethod = "GET";
        String headers = String.format("accept: application/json; content-type: application/json; authorization: Bearer %s", hubspotToken);
        String changeurlHubspot = urlHubspot;
        boolean moreResults = true;
        double totalFTEs = 0.0;

        JSONArray allProperties = new JSONArray();

        log.info("Fetching data from {}", urlHubspot);

        try {
            while (moreResults) {
                Collection<JSONArray> rawResponse = planBuilder
                        .readRestAPISource(changeurlHubspot, apiMethod, headers)
                        .collect();

                for (Object element : rawResponse) {
                    if (element instanceof JSONObject) {
                        JSONObject jsonObject = (JSONObject) element;
                        if (jsonObject.has("results")) {
                            JSONArray resultsArray = jsonObject.getJSONArray("results");
                            for (Object resultElement : resultsArray) {
                                if (resultElement instanceof JSONObject) {
                                    JSONObject resultObject = (JSONObject) resultElement;
                                    if (resultObject.has("properties")) {
                                        allProperties.put(resultObject.getJSONObject("properties"));
                                    }
                                }
                            }
                        }
                    }
                }

                log.info("Fetched {} responses in this page", rawResponse.size());

                String after = extractAfterToken(rawResponse);
                if (after != null) {
                    changeurlHubspot = urlHubspot + "&after=" + after;
                } else {
                    moreResults = false;
                }
            }

            for (Object obj : allProperties) {
                if (obj instanceof JSONObject) {
                    JSONObject deal = (JSONObject) obj;

                    if (deal.has("start_date") && !deal.isNull("start_date") && deal.has("end_date") && !deal.isNull("end_date")) {
                        String startDateStr = deal.getString("start_date");
                        String endDateStr = deal.getString("end_date");

                        try {
                            LocalDate startDate = LocalDate.parse(startDateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                            LocalDate endDate = LocalDate.parse(endDateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                            LocalDate filterDate = LocalDate.parse(monthToday, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                            YearMonth startMonth = YearMonth.from(startDate);
                            YearMonth filterMonth = YearMonth.from(filterDate);

                            if (startMonth.equals(filterMonth)) {
                                long monthsBetween = ChronoUnit.MONTHS.between(startDate, endDate);
                                if (monthsBetween <= 0) {
                                    log.warn("Deal has invalid months range (<= 0 months): {}", deal);
                                    monthsBetween = 1;
                                }

                                if (deal.has("fte_s_") && !deal.isNull("fte_s_")) {
                                    double fteValue = deal.getDouble("fte_s_");
                                    totalFTEs += fteValue / monthsBetween;
                                }
                            }
                        } catch (Exception e) {
                            // log.error("Error parsing dates for deal: {}. Skipping.", deal, e);
                        }
                    }
                }
            }

            // log.info("Total FTEs from Hubspot: {}", totalFTEs);
        } catch (Exception e) {
            log.error("Error fetching data from Hubspot API: {}", e.getMessage(), e);
        }

        return totalFTEs;
    }

    private static String extractAfterToken(Collection<JSONArray> rawResponse) {
        for (Object element : rawResponse) {
            if (element instanceof JSONObject) {
                JSONObject jsonObject = (JSONObject) element;
                if (jsonObject.has("paging")) {
                    JSONObject paging = jsonObject.getJSONObject("paging");
                    if (paging.has("next")) {
                        JSONObject next = paging.getJSONObject("next");
                        return next.getString("after");
                    }
                }
            }
        }
        return null;
    }
}

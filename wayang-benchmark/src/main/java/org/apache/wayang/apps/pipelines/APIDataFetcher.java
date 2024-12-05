package org.apache.wayang.apps.pipelines;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class APIDataFetcher {

    private String apiURL;
    private String apiMethod;
    private String headers;
    private static final Logger logger = LoggerFactory.getLogger(APIDataFetcher.class);

    public APIDataFetcher(String apiURL, String apiMethod, String headers) {
        this.apiURL = apiURL;
        this.apiMethod = apiMethod;
        this.headers = headers;
    }

    public JSONArray fetchDataFromAPI() {
        String hardcodedURL = "https://api.hubapi.com/crm/v3/objects/deals/search";
        logger.info("Fetching data from API with method: {}", this.apiMethod);
        HttpURLConnection connection = null;
        try {
            if ("POST".equalsIgnoreCase(this.apiMethod) && !hardcodedURL.equals(this.apiURL)) {
                logger.error("POST requests are only allowed to the hardcoded URL: {}", hardcodedURL);
                throw new IllegalArgumentException("POST requests must use the hardcoded URL.");
            }

            URL url = new URL(this.apiURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(this.apiMethod);

            if (!this.headers.isEmpty()) {
                for (String header : this.headers.split(";")) {
                    String[] headerParts = header.trim().split(":", 2);
                    if (headerParts.length == 2) {
                        connection.setRequestProperty(headerParts[0].trim(), headerParts[1].trim());
                    } else {
                        logger.warn("Invalid header format: {}", header);
                    }
                }
            }

            if ("POST".equalsIgnoreCase(this.apiMethod)) {
                connection.setDoOutput(true);
                String payload = getPayload();
                if (payload == null || payload.isEmpty()) {
                    logger.warn("No payload provided for POST request.");
                } else {
                    try (OutputStream os = connection.getOutputStream()) {
                        byte[] input = payload.getBytes("utf-8");
                        os.write(input, 0, input.length);
                    }
                }
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder content = new StringBuilder();
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine).append("\n");
            }
            in.close();

            String response = content.toString();

            try {
                logger.info("Attempting to parse response as JSONArray.");
                return new JSONArray(response);
            } catch (JSONException e) {
                logger.info("Response is not a JSONArray. Trying as JSONObject.");
            }

            try {
                JSONObject jsonObject = new JSONObject(response);
                JSONArray jsonArray = new JSONArray();
                jsonArray.put(jsonObject);
                return jsonArray;
            } catch (JSONException e) {
                logger.info("Response is not a JSONObject. Trying as CSV string.");
            }

            try {
                return convertCsvToJson(response);
            } catch (Exception e) {
                logger.error("Failed to parse response as CSV string.", e);
            }

        } catch (IOException e) {
            logger.error("Unable to fetch data from REST API", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return new JSONArray();
    }

    private JSONArray convertCsvToJson(String dataString) {
        String[] lines = dataString.split("\n");

        if (lines.length == 0) {
            return new JSONArray();
        }

        String[] columns = lines[0].split(",");

        JSONArray jsonArray = new JSONArray();

        for (int i = 1; i < lines.length; i++) {
            if (lines[i].trim().isEmpty()) {
                continue;
            }

            String[] values = lines[i].split(",");
            JSONObject jsonObject = new JSONObject();

            for (int j = 0; j < columns.length; j++) {
                String columnName = columns[j].trim();
                String value = j < values.length ? values[j].trim() : "";
                jsonObject.put(columnName, value);
            }

            jsonArray.put(jsonObject);
        }

        return jsonArray;
    }

    private String getPayload() {
        // Replace with actual logic to get payload
        return "{\"sampleKey\": \"sampleValue\"}";
    }

    // Main method to test the implementation
    public static void main(String[] args) {
        APIDataFetcher fetcher = new APIDataFetcher(
            "https://api.hubapi.com/crm/v3/objects/deals/search",
            "POST",
            "Authorization: Bearer your-token;Content-Type: application/json"
        );

        JSONArray response = fetcher.fetchDataFromAPI();
        System.out.println(response.toString(2)); // Pretty-print JSON
    }
}

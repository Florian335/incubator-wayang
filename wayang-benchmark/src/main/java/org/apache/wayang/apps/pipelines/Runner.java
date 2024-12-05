package org.apache.wayang.apps.pipelines;

import java.io.*;
import java.lang.management.ManagementFactory;
// import java.lang.management.OperatingSystemMXBean;
import com.sun.management.OperatingSystemMXBean;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

public class Runner {

    private static class ResourceMonitor implements Runnable {
        private final BlockingQueue<JSONObject> dataQueue;
        private final String scriptName;
        private volatile boolean stop;

        public ResourceMonitor(BlockingQueue<JSONObject> dataQueue, String scriptName) {
            this.dataQueue = dataQueue;
            this.scriptName = scriptName;
            this.stop = false;
        }

        public void stopMonitoring() {
            this.stop = true;
        }

        @Override
        public void run() {
            OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();            
            Runtime runtime = Runtime.getRuntime();

            while (!stop) {
                try {
                    double cpuLoad = osBean.getProcessCpuLoad() * 100;
                    double memoryUsed = (double) (runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory();

                    if (cpuLoad >= 0 && memoryUsed >= 0) {
                        JSONObject data = new JSONObject();
                        data.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
                        data.put("script", scriptName);
                        data.put("cpu_percent", cpuLoad);
                        data.put("memory_percent", memoryUsed);

                        dataQueue.put(data);
                    }

                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("Error collecting resource data: " + e.getMessage());
                }
            }
        }
    }

    private static void runScripts() {
        String[][] scripts = {
                {"org.apache.wayang.apps.pipelines.PipelinePOST", "PipelinePOST.java"}
        };

        for (String[] script : scripts) {
            String className = script[0];
            String scriptName = script[1];
            BlockingQueue<JSONObject> resourceData = new LinkedBlockingQueue<>();
            ResourceMonitor monitor = new ResourceMonitor(resourceData, scriptName);
            Thread monitorThread = new Thread(monitor);

            try {
                monitorThread.start();

                Class<?> clazz = Class.forName(className);
                clazz.getMethod("main", String[].class).invoke(null, (Object) new String[]{});

                monitor.stopMonitoring();
                monitorThread.join();

                writeDataToJson(resourceData, "resource_usage.json");

                System.out.printf("%s ran successfully.%n", scriptName);
            } catch (Exception e) {
                System.err.printf("An error occurred while running %s: %s%n", scriptName, e);
                e.printStackTrace();
            } finally {
                monitor.stopMonitoring();
                monitorThread.interrupt();
            }
        }
    }

    private static void writeDataToJson(BlockingQueue<JSONObject> dataQueue, String filePath) {
        try {
            // Prepare a list for new data
            List<JSONObject> newData = new ArrayList<>();

            // Read and parse existing data
            Path path = Paths.get(filePath);
            if (Files.exists(path)) {
                String content = Files.readString(path);
                if (!content.isBlank()) { // Ensure the content is not empty or invalid
                    JSONArray jsonArray = new JSONArray(content);
                    for (int i = 0; i < jsonArray.length(); i++) {
                        newData.add(jsonArray.getJSONObject(i));
                    }
                }
            }

            // Append new data from the queue
            while (!dataQueue.isEmpty()) {
                JSONObject data = dataQueue.poll();
                if (data != null) { // Ensure data is valid
                    newData.add(data);
                }
            }

            // Write all data back to the file
            JSONArray outputArray = new JSONArray(newData);
            Files.writeString(path, outputArray.toString(4), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            System.err.printf("Error writing data to JSON: %s%n", e.getMessage());
        }
    }


    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            System.out.printf("--- Iteration %d ---%n", i + 1);
            runScripts();
        }
    }
}

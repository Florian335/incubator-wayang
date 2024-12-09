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
* Unless required by applicable law or agreed to in writing,
* software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import org.json.JSONArray;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Date;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.avro.generic.GenericRecord;

public class JavaParquetSource extends ParquetSource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaParquetSource.class);
    private static final String LOG_FILE_PATH = "read-latency.json";

    public static void logtoJSON(String stepname, Double latencyseconds) {
        try {
            JSONObject logrecord = new JSONObject();
            logrecord.put("timestamp", Date.from(Instant.now()).toString());
            logrecord.put("step", stepname);
            logrecord.put("latency_seconds", latencyseconds);

            appendlogtofile(logrecord);
        } catch (Exception e) {
            logger.error("Unable to add data to JSON: {}", e.getMessage(), e);
        }
    }

    public static void logAPIlatency(long starttime, long endtime, String stepname) {
        Double latencyseconds = (endtime - starttime) / 1000.0;
        logtoJSON(stepname, latencyseconds);
    }

    private static void appendlogtofile(JSONObject logrecord) throws IOException {
        JSONArray existinglogs;

        if (Files.exists(Paths.get(LOG_FILE_PATH))) {
            String content = new String(Files.readAllBytes(Paths.get(LOG_FILE_PATH)));
            if (!content.isEmpty()) {
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

    public JavaParquetSource(ParquetSource parquetsource) {
        super(parquetsource.getPath());
    }

    public JavaParquetSource(String path) {
        super(path);
    }

    public String readParquet() {
        StringBuilder parquetContent = new StringBuilder();
    
        try {
            InputFile inputFile = new InputFile() {
                @Override
                public long getLength() throws IOException {
                    return new File(JavaParquetSource.this.getPath()).length();
                }
    
                @Override
                public SeekableInputStream newStream() throws IOException {
                    FileInputStream fileInputStream = new FileInputStream(JavaParquetSource.this.getPath());
                    return new DelegatingSeekableInputStream(fileInputStream) {
                        @Override
                        public void seek(long position) throws IOException {
                            fileInputStream.getChannel().position(position);
                        }
    
                        @Override
                        public long getPos() throws IOException {
                            return fileInputStream.getChannel().position();
                        }
    
                        @Override
                        public void close() throws IOException {
                            super.close();
                            fileInputStream.close(); // Ensure the underlying stream is closed
                        }
                    };
                }
            };
    
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                GenericRecord record;
    
                while ((record = reader.read()) != null) {
                    parquetContent.append(record.toString()).append("\n");
                }
            }
        } catch (IOException e) {
            logger.error("Error reading Parquet file: {}", this.getPath(), e);
            throw new WayangException("Failed to read Parquet file.", e);
        }
    
        return parquetContent.toString();
    }
    
    
    

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        try {
            String parquetText = readParquet();
            Stream<String> lines = Arrays.stream(parquetText.split("\n"));
            ((StreamChannel.Instance) outputs[0]).accept(lines);
            logger.info("Successfully streamed data from Parquet: {}", this.getPath());
        } catch (Exception e) {
            logger.error("Failed to fetch data from Parquet {}", this.getPath(), e);
            throw new WayangException("Failed to fetch data from Parquet.", e);
        }

        return new Tuple<>(Collections.emptyList(), Arrays.asList(outputs));
    }

    @Override
    public JavaParquetSource copy() {
        return new JavaParquetSource(this);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.parquetsource.load.prepare", "wayang.java.parquetsource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}

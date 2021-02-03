package org.apache.wayang.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.util.fs.LocalFileSystem;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JavaTextFileSink}.
 */
public class JavaTextFileSinkTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testWritingLocalFile() throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();

        final File tempDir = LocalFileSystem.findTempDir();
        final String targetUrl = LocalFileSystem.toURL(new File(tempDir, "testWritingLocalFile.txt"));
        JavaTextFileSink<Float> sink = new JavaTextFileSink<>(
                targetUrl,
                new TransformationDescriptor<>(
                        f -> String.format("%.2f", f),
                        Float.class, String.class
                )
        );

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance inputChannelInstance = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);
        inputChannelInstance.accept(Stream.of(1.123f, -0.1f, 3f));
        evaluate(sink, new ChannelInstance[]{inputChannelInstance}, new ChannelInstance[0]);


        final List<String> lines = Files.lines(Paths.get(new URI(targetUrl))).collect(Collectors.toList());
        Assert.assertEquals(
                Arrays.asList("1.12", "-0.10", "3.00"),
                lines
        );

    }

}

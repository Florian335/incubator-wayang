package org.apache.wayang.tests;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.LoadCollectionDataQuantaBuilder;
import org.apache.wayang.api.MapDataQuantaBuilder;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.util.WayangArrays;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class hosts and documents some tests for bugs that we encountered. Ultimately, we want to avoid re-introducing
 * already encountered and fixed bugs.
 */
public class RegressionIT {

    /**
     * This plan revealed an issue with the {@link org.apache.wayang.core.optimizer.channels.ChannelConversionGraph.ShortestTreeSearcher}.
     */
    @Test
    public void testCollectionToRddAndBroadcast() {
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin()).with(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext, "testCollectionToRddAndBroadcast");

        LoadCollectionDataQuantaBuilder<String> collection = planBuilder
                .loadCollection(Arrays.asList("a", "bc", "def"))
                .withTargetPlatform(Java.platform())
                .withName("collection");

        MapDataQuantaBuilder<String, Integer> map1 = collection
                .map(String::length)
                .withTargetPlatform(Spark.platform());

        MapDataQuantaBuilder<Integer, Integer> map2 = planBuilder
                .loadCollection(WayangArrays.asList(-1))

                .map(i -> i)
                .withBroadcast(collection, "broadcast")
                .withTargetPlatform(Spark.platform());

        ArrayList<Integer> result = new ArrayList<>(map1.union(map2).collect());

        result.sort(Integer::compareTo);
        Assert.assertEquals(
                WayangArrays.asList(-1, 1, 2, 3),
                result
        );
    }


}

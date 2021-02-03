package org.apache.wayang.basic.operators;

import org.junit.Test;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;

import java.util.stream.StreamSupport;

/**
 * Tests for the {@link MaterializedGroupByOperator}.
 */
public class MaterializedGroupByOperatorTest {

    @Test
    public void testConnectingToMap() {
        final MaterializedGroupByOperator<String, Integer> materializedGroupByOperator =
                new MaterializedGroupByOperator<>(String::length, String.class, Integer.class);
        final MapOperator<Iterable<String>, Integer> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(
                        strings -> (int) StreamSupport.stream(strings.spliterator(), false).count(),
                        DataUnitType.createBasicUnchecked(Iterable.class),
                        DataUnitType.createBasic(Integer.class)
                ),
                DataSetType.createGrouped(String.class),
                DataSetType.createDefault(Integer.class)
        );
        materializedGroupByOperator.connectTo(0, mapOperator, 0);
    }

}

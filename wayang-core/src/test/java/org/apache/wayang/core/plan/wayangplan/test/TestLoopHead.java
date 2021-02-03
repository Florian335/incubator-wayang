package org.apache.wayang.core.plan.wayangplan.test;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.SwitchForwardCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.LoopHeadOperator;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * {@link LoopHeadOperator} implementation for test purposes.
 */
public class TestLoopHead<T> extends OperatorBase implements LoopHeadOperator, ElementaryOperator {

    private int numExpectedIterations;

    public TestLoopHead(Class<T> dataQuantumClass) {
        super(2, 2, false);

        final DataSetType<T> dataSetType = DataSetType.createDefault(DataUnitType.createBasic(dataQuantumClass));
        this.inputSlots[0] = new InputSlot<>("initialInput", this, dataSetType);
        this.inputSlots[1] = new InputSlot<>("loopInput", this, dataSetType);
        this.outputSlots[0] = new OutputSlot<>("loopOutput", this, dataSetType);
        this.outputSlots[1] = new OutputSlot<>("finalOutput", this, dataSetType);
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return Collections.singleton(this.getOutput("loopOutput"));
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return Collections.singleton(this.getOutput("finalOutput"));
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return Collections.singleton(this.getInput("loopInput"));
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return Collections.singleton(this.getInput("initialInput"));
    }

    @Override
    public Collection<InputSlot<?>> getConditionInputSlots() {
        return Collections.emptyList();
    }

    @Override
    public Collection<OutputSlot<?>> getConditionOutputSlots() {
        return Collections.emptyList();
    }

    @Override
    public int getNumExpectedIterations() {
        return this.numExpectedIterations;
    }

    public void setNumExpectedIterations(int numExpectedIterations) {
        this.numExpectedIterations = numExpectedIterations;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        switch (outputIndex) {
            case 0:
            case 1:
                return Optional.of(new SwitchForwardCardinalityEstimator(0, 1));
            default:
                throw new IllegalArgumentException("Illegal output index " + outputIndex + ".");
        }
    }
}

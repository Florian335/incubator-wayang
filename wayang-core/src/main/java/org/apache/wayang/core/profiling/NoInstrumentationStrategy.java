package org.apache.wayang.core.profiling;

import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;

/**
 * Instruments only outbound {@link Channel}s.
 */
@SuppressWarnings("unused") // Can be activated via Configuration.
public class NoInstrumentationStrategy implements InstrumentationStrategy {

    @Override
    public void applyTo(ExecutionStage stage) {
        // Nothing will be instrumented.
    }
}

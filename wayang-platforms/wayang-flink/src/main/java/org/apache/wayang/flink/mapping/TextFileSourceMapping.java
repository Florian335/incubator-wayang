package org.apache.wayang.flink.mapping;

import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.flink.operators.FlinkTextFileSource;
import org.apache.wayang.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TextFileSource} to {@link FlinkTextFileSource}.
 */
@SuppressWarnings("unchecked")
public class TextFileSourceMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                FlinkPlatform.getInstance()
        ));
    }


    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new TextFileSource("", null), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TextFileSource>(
                (matchedOperator, epoch) -> new FlinkTextFileSource(matchedOperator).at(epoch)
        );
    }
}

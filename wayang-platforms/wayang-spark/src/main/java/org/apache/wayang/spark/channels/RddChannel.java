package org.apache.wayang.spark.channels;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.util.Actions;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.OptionalLong;

/**
 * Describes the situation where one {@link JavaRDD} is operated on, producing a further {@link JavaRDD}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>
 */
public class RddChannel extends Channel {

    public static final ChannelDescriptor UNCACHED_DESCRIPTOR = new ChannelDescriptor(
            RddChannel.class, false, false
    );

    public static final ChannelDescriptor CACHED_DESCRIPTOR = new ChannelDescriptor(
            RddChannel.class, true, true
    );

    public RddChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == UNCACHED_DESCRIPTOR || descriptor == CACHED_DESCRIPTOR;
    }

    private RddChannel(RddChannel parent) {
        super(parent);
    }

    @Override
    public RddChannel copy() {
        return new RddChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance((SparkExecutor) executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelInstance} implementation for {@link RddChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private JavaRDD<?> rdd;

        private Accumulator<Integer> accumulator;

        public Instance(SparkExecutor executor,
                        OptimizationContext.OperatorContext producerOperatorContext,
                        int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(JavaRDD<?> rdd, SparkExecutor sparkExecutor) throws WayangException {
            if (this.isMarkedForInstrumentation() && !this.isRddCached()) {
                final Accumulator<Integer> accumulator = sparkExecutor.sc.accumulator(0);
                this.rdd = rdd.filter(dataQuantum -> {
                    accumulator.add(1);
                    return true;
                });
                this.accumulator = accumulator;
            } else {
                this.rdd = rdd;
            }
        }


        @SuppressWarnings("unchecked")
        public <T> JavaRDD<T> provideRdd() {
            return (JavaRDD<T>) this.rdd;
        }

        @Override
        protected void doDispose() {
            if (this.accumulator != null) {
                this.setMeasuredCardinality(this.accumulator.value());
                this.accumulator = null;
            }
            if (this.isRddCached() && this.rdd != null) {
                Actions.doSafe(this.rdd::unpersist);
                logger.debug("Unpersisted {}.", this.rdd);
                this.rdd = null;
            }
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            if (this.accumulator != null) {
                this.setMeasuredCardinality(this.accumulator.value());
            }
            return super.getMeasuredCardinality();
        }

        @Override
        public RddChannel getChannel() {
            return RddChannel.this;
        }

        private boolean isRddCached() {
            return this.getChannel().isReusable();
        }
    }

}

package org.apache.wayang.core.api;

import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import org.apache.wayang.core.monitor.Monitor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.profiling.CardinalityRepository;
import org.apache.wayang.core.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the entry point for users to work with Wayang.
 */
public class WayangContext {

    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Stores input/output cardinalities to provide better {@link CardinalityEstimator}s over time.
     */
    private CardinalityRepository cardinalityRepository;

    private final Configuration configuration;

    public WayangContext() {
        this(new Configuration());
    }

    public WayangContext(Configuration configuration) {
        this.configuration = configuration.fork(String.format("WayangContext(%s)", configuration.getName()));
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @return this instance
     */
    public WayangContext with(Plugin plugin) {
        this.register(plugin);
        return this;
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @return this instance
     */
    public WayangContext withPlugin(Plugin plugin) {
        this.register(plugin);
        return this;
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @see #with(Plugin)
     */
    public void register(Plugin plugin) {
        plugin.configure(this.getConfiguration());
    }

    /**
     * Execute a plan.
     *
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(WayangPlan wayangPlan, String... udfJars) {
        this.execute(null, wayangPlan, udfJars);
    }

    /**
     * Execute a plan.
     *
     * @param jobName   name of the {@link Job} or {@code null}
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, WayangPlan wayangPlan, String... udfJars) {
        this.execute(jobName, null, wayangPlan, udfJars);
    }

    /**
     * Execute a plan.
     *
     * @param jobName   name of the {@link Job} or {@code null}
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, Monitor monitor, WayangPlan wayangPlan, String... udfJars) {
        this.createJob(jobName, monitor, wayangPlan, udfJars).execute();
    }

    /**
     * Execute a plan.
     *
     * @param jobName    name of the {@link Job} or {@code null}
     * @param wayangPlan  the plan to execute
     * @param experiment {@link Experiment} for that profiling entries will be created
     * @param udfJars    JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, WayangPlan wayangPlan, Experiment experiment, String... udfJars) {
        this.createJob(jobName, wayangPlan, experiment, udfJars).execute();
    }


    /**
     * Build an execution plan.
     *
     * @param wayangPlan the plan to translate
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public ExecutionPlan buildInitialExecutionPlan(String jobName, WayangPlan wayangPlan, String... udfJars) {
        return this.createJob(jobName, null, wayangPlan, udfJars).buildInitialExecutionPlan();
    }

    /**
     * Create a new {@link Job} that should execute the given {@link WayangPlan} eventually.
     *
     * @param experiment {@link Experiment} for that profiling entries will be created
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, WayangPlan wayangPlan, Experiment experiment, String... udfJars) {
        return new Job(this, jobName, null, wayangPlan, experiment, udfJars);
    }

    /**
     * Create a new {@link Job} that should execute the given {@link WayangPlan} eventually.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, WayangPlan wayangPlan, String... udfJars) {
        return this.createJob(jobName, null, wayangPlan, udfJars);
    }

    /**
     * Create a new {@link Job} that should execute the given {@link WayangPlan} eventually.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, Monitor monitor, WayangPlan wayangPlan, String... udfJars) {
        return new Job(this, jobName, monitor, wayangPlan, new Experiment("unknown", new Subject("unknown", "unknown")), udfJars);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public CardinalityRepository getCardinalityRepository() {
        if (this.cardinalityRepository == null) {
            this.cardinalityRepository = new CardinalityRepository(this.configuration);
        }
        return this.cardinalityRepository;
    }
}

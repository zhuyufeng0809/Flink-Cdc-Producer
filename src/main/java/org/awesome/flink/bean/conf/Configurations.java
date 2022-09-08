package org.awesome.flink.bean.conf;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.awesome.flink.util.JsonConvertor;

import java.util.List;

public class Configurations {
    @JsonProperty(value = "default-flink-kafka-sink")
    private FlinkKafkaSinkConf defaultFlinkKafkaSinkConf;
    private List<Branch> branches;

    public Configurations() {
    }

    public Configurations(FlinkKafkaSinkConf defaultFlinkKafkaSinkConf, List<Branch> branches) {
        this.defaultFlinkKafkaSinkConf = defaultFlinkKafkaSinkConf;
        this.branches = branches;
    }

    public FlinkKafkaSinkConf getDefaultFlinkKafkaSinkConf() {
        return defaultFlinkKafkaSinkConf;
    }

    public Configurations setDefaultFlinkKafkaSinkConf(FlinkKafkaSinkConf defaultFlinkKafkaSinkConf) {
        this.defaultFlinkKafkaSinkConf = defaultFlinkKafkaSinkConf;
        return this;
    }

    public List<Branch> getBranches() {
        return branches;
    }

    public Configurations setBranches(List<Branch> branches) {
        this.branches = branches;
        return this;
    }

    public void applyDefaultFlinkConf(StreamExecutionEnvironment env) {
        applyDefaultCheckPointConf(env);
        applyDefaultSavePointConf(env);
        applyDefaultStateBackendConf(env);
        applyDefaultRestartStrategyConf(env);
    }

    private void applyDefaultCheckPointConf(StreamExecutionEnvironment env) {
        /*
        Only the most basic options are configured here,
        more CheckPoint options should be configured in the Flink Job commit command
         */
        /*
        Cause flink kafka sink use transaction to commit data to kafka under exactly-once mode,
        so checkpoint interval should not be too long
         */
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
    }

    private void applyDefaultSavePointConf(StreamExecutionEnvironment env) {
        //
    }

    private void applyDefaultStateBackendConf(StreamExecutionEnvironment env) {
        /*
         this kind of flink job should not use too much state,
         so default use hash-map stateBackend,
         cause do not have group or join operation, recommend to set managed memory to zero
         */
        env.setStateBackend(new HashMapStateBackend());
    }

    private void applyDefaultRestartStrategyConf(StreamExecutionEnvironment env) {
        /*
        default use no-restart strategy,
        because most exceptions are unrecoverable exceptions,
        when it appears, job should break down, and require manual intervention rather than constant restarting
         */
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    public static Configurations fromJson(String[] args) throws Exception {
        String jsonFilePath = ParameterTool.fromArgs(args).getRequired("jsonConf");
        return JsonConvertor.convertFromJsonFile(jsonFilePath, Configurations.class);
    }
}

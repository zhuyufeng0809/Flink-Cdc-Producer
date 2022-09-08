package org.awesome.flink.bean.conf;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;

public class Branch {

    @JsonProperty(value = "branch-id")
    private String branchId;
    private Integer parallelism;
    @JsonProperty(value = "flink-cdc-source")
    private FlinkCdcSourceConf flinkCdcSourceConf;
    @JsonProperty(value = "flink-kafka-sink")
    private FlinkKafkaSinkConf flinkKafkaSinkConf;

    public Branch() {
    }

    public Branch(String branchId, Integer parallelism, FlinkCdcSourceConf flinkCdcSourceConf, FlinkKafkaSinkConf flinkKafkaSinkConf) {
        this.branchId = branchId;
        this.parallelism = parallelism;
        this.flinkCdcSourceConf = flinkCdcSourceConf;
        this.flinkKafkaSinkConf = flinkKafkaSinkConf;
    }

    public MySqlSource<String> buildMysqlCdcSource() throws Exception {
        MySqlSourceBuilder<String> mySqlSourceBuilder = MySqlSource.builder();
        flinkCdcSourceConf.applyConf(mySqlSourceBuilder);
        return mySqlSourceBuilder.build();
    }

    public KafkaSink<String> buildKafkaSink(FlinkKafkaSinkConf defaultFlinkKafkaSinkConf) throws Exception {
        String instance = flinkCdcSourceConf.getInstance();
        KafkaSinkBuilder<String> kafkaSinkBuilder = KafkaSink.builder();
        if (flinkKafkaSinkConf == null) {
            defaultFlinkKafkaSinkConf.applyConf(kafkaSinkBuilder, instance);
        } else {
            flinkKafkaSinkConf.applyConf(kafkaSinkBuilder, instance);
        }
        return kafkaSinkBuilder.build();
    }

    public String getBranchId() {
        return branchId;
    }

    public Branch setBranchId(String branchId) {
        this.branchId = branchId;
        return this;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public Branch setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkCdcSourceConf getFlinkCdcSourceConf() {
        return flinkCdcSourceConf;
    }

    public Branch setFlinkCdcSourceConf(FlinkCdcSourceConf flinkCdcSourceConf) {
        this.flinkCdcSourceConf = flinkCdcSourceConf;
        return this;
    }

    public FlinkKafkaSinkConf getFlinkKafkaSinkConf() {
        return flinkKafkaSinkConf;
    }

    public Branch setFlinkKafkaSinkConf(FlinkKafkaSinkConf flinkKafkaSinkConf) {
        this.flinkKafkaSinkConf = flinkKafkaSinkConf;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Branch branch = (Branch) o;
        return Objects.equal(branchId, branch.branchId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(branchId);
    }

    @Override
    public String toString() {
        return "Branch{" +
                "branchId='" + branchId + '\'' +
                ", parallelism=" + parallelism +
                ", flinkCdcSourceConf=" + flinkCdcSourceConf +
                ", flinkKafkaSinkConf=" + flinkKafkaSinkConf +
                '}';
    }
}
